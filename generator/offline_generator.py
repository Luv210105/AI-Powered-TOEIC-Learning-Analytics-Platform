"""
offline_generator.py — Data Generator offline cho AI-Powered TOEIC Learning Analytics Platform.

Sinh 7 bảng TOEIC dạng Parquet (ghi vào data/offline/), CỐ TÌNH cài 4 lỗi dữ liệu offline:
  1. Skew            — dồn mục tiêu 600-700 (users) + dồn Part 5 (questions/answers)
  2. High cardinality— các ID gần như duy nhất (user_id, question_id, attempt_id, answer_id)
  3. Schema evolution— partition attempt_answers TRƯỚC schema_change_date thiếu 2 cột difficulty + tag_grammar
  4. Duplicate       — ~2% dòng attempt_answers trùng business key (attempt_id, question_id)

Mọi tham số đọc từ generator/config.yaml. Chạy: uv run python generator/offline_generator.py
"""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Đọc file config.yaml, trả về dict tham số."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def sigmoid(x: np.ndarray) -> np.ndarray:
    """Hàm sigmoid: đưa giá trị thực về xác suất trong khoảng (0, 1)."""
    return 1.0 / (1.0 + np.exp(-x))


def gen_users(cfg, rng, start_date, end_date) -> pd.DataFrame:
    """Sinh bảng users. Cài SKEW: ~80% học viên có mục tiêu trong band 600-700."""
    n = cfg["n_users"]
    in_band = rng.random(n) < cfg["skew_target_score_band"]
    target = np.where(
        in_band,
        rng.choice([600, 650, 700], size=n),                 # 80% dồn vào đây
        rng.choice([300, 400, 500, 800, 900, 990], size=n),  # 20% rải ra
    )
    span_sec = int((end_date - start_date).total_seconds())
    signup_ts = start_date + pd.to_timedelta(rng.integers(0, span_sec, n), unit="s")
    df = pd.DataFrame({
        "user_id": np.arange(1, n + 1),
        "signup_ts": signup_ts,
        "age": rng.integers(18, 31, n),
        "university": rng.choice(["UET", "HUST", "FTU", "NEU", "RMIT", "UEH"], n),
        "major": rng.choice(["CS", "Business", "Econ", "Eng", "Law"], n),
        "target_score": target,
        "base_ability": rng.normal(cfg["ability_start_mean"], 0.5, n),
        "created_ts": pd.Timestamp.now(),
    })
    df["signup_date"] = df["signup_ts"].dt.date.astype("string")
    return df


def gen_questions(cfg, rng) -> pd.DataFrame:
    """Sinh ngân hàng câu hỏi. Cài SKEW: ~35% câu thuộc Part 5."""
    n = cfg["n_questions"]
    is_part5 = rng.random(n) < cfg["skew_part5_ratio"]
    part = np.where(is_part5, 5, rng.choice([1, 2, 3, 4, 6, 7], size=n))
    return pd.DataFrame({
        "question_id": np.arange(1, n + 1),
        "part": part,
        "skill": np.where(part <= 4, "listening", "reading"),
        "difficulty": np.round(rng.beta(2, 2, n), 3),   # 0..1, tập trung quanh 0.5
        "tag_grammar": rng.choice(["tense", "prep", "conj", "pronoun", "voice", "none"], n),
        "correct_answer": rng.choice(["A", "B", "C", "D"], n),
        "created_ts": pd.Timestamp.now(),
    })


def gen_vocabulary(cfg, rng) -> pd.DataFrame:
    """Sinh bảng từ vựng."""
    n = cfg["n_vocabulary"]
    return pd.DataFrame({
        "word_id": np.arange(1, n + 1),
        "word": [f"word_{i}" for i in range(1, n + 1)],
        "topic": rng.choice(["business", "travel", "office", "daily", "finance"], n),
        "difficulty_level": rng.integers(1, 6, n),
        "created_ts": pd.Timestamp.now(),
    })


def gen_grammar_topics(cfg, rng) -> pd.DataFrame:
    """Sinh bảng chủ đề ngữ pháp."""
    n = cfg["n_grammar_topics"]
    return pd.DataFrame({
        "grammar_id": np.arange(1, n + 1),
        "topic_name": [f"grammar_topic_{i}" for i in range(1, n + 1)],
        "difficulty_level": rng.integers(1, 6, n),
        "created_ts": pd.Timestamp.now(),
    })


def gen_mock_tests(cfg, rng) -> pd.DataFrame:
    """Sinh bảng định nghĩa bộ đề."""
    n = cfg["n_mock_tests"]
    return pd.DataFrame({
        "test_id": np.arange(1, n + 1),
        "test_name": [f"Mock Test {i}" for i in range(1, n + 1)],
        "total_questions": cfg["questions_per_attempt"],
        "created_ts": pd.Timestamp.now(),
    })


def gen_attempts_and_answers(cfg, rng, users, questions, start_date):
    """Sinh test_attempts + attempt_answers (vectorized).

    - Mỗi user ~Poisson(avg_attempts_per_user) lượt thi (tối thiểu 1).
    - Mỗi lượt questions_per_attempt câu; P(đúng)=sigmoid(ability - difficulty + noise).
    - ability tăng dần theo thời gian -> tạo tín hiệu học tập.
    Trả về (attempts_df, answers_df); answers đã denormalize part/difficulty/tag_grammar.
    """
    qpa = cfg["questions_per_attempt"]
    days = cfg["days_history"]

    # ---- Số lượt thi mỗi user ----
    n_per_user = np.maximum(rng.poisson(cfg["avg_attempts_per_user"], len(users)), 1)
    total_attempts = int(n_per_user.sum())
    attempt_id = np.arange(1, total_attempts + 1)
    user_id = np.repeat(users["user_id"].to_numpy(), n_per_user)
    base_ability = np.repeat(users["base_ability"].to_numpy(), n_per_user)
    signup_off = np.repeat((users["signup_ts"] - start_date).dt.days.to_numpy(), n_per_user)

    # ---- Thời điểm mỗi lượt (phải sau ngày signup) ----
    day_off = np.array([rng.integers(s, days) if s < days - 1 else days - 1 for s in signup_off])
    start_ts = pd.Series(start_date + pd.to_timedelta(day_off, unit="D")
                         + pd.to_timedelta(rng.integers(0, 86400, total_attempts), unit="s"))
    submit_ts = start_ts + pd.Series(pd.to_timedelta(rng.integers(600, 3600, total_attempts), unit="s"))

    attempts = pd.DataFrame({
        "attempt_id": attempt_id,
        "user_id": user_id,
        "test_id": rng.integers(1, cfg["n_mock_tests"] + 1, total_attempts),
        "start_ts": start_ts,
        "submit_ts": submit_ts,
    })

    # ---- Answers: mỗi attempt qpa câu ----
    total_ans = total_attempts * qpa
    q_idx = rng.integers(0, len(questions), total_ans)      # chọn câu hỏi (có thể lặp)
    ability = np.repeat(base_ability + cfg["ability_growth"] * (day_off / days), qpa)
    q_diff = questions["difficulty"].to_numpy()[q_idx]
    q_correct = questions["correct_answer"].to_numpy()[q_idx]

    p_correct = sigmoid(ability - q_diff + rng.normal(0, cfg["noise_std"], total_ans))
    is_correct = rng.random(total_ans) < p_correct

    # user_answer: đúng -> đáp án đúng; sai -> 1 đáp án khác (correct_idx + 1..3)
    letters = np.array(["A", "B", "C", "D"])
    correct_idx = np.searchsorted(letters, q_correct)
    ans_idx = np.where(is_correct, correct_idx, (correct_idx + rng.integers(1, 4, total_ans)) % 4)

    attempt_date = np.repeat(start_ts.dt.date.astype("string").to_numpy(), qpa)
    answers = pd.DataFrame({
        "answer_id": np.arange(1, total_ans + 1),
        "attempt_id": np.repeat(attempt_id, qpa),
        "question_id": questions["question_id"].to_numpy()[q_idx],
        "part": questions["part"].to_numpy()[q_idx],
        "user_answer": letters[ans_idx],
        "is_correct": is_correct,
        "time_spent_sec": rng.integers(5, 120, total_ans),
        "difficulty": q_diff,                                        # denormalized (bỏ ở partition cũ)
        "tag_grammar": questions["tag_grammar"].to_numpy()[q_idx],   # denormalized (bỏ ở partition cũ)
        "attempt_date": attempt_date,
    })

    # ---- Tính điểm listening/reading cho mỗi attempt ----
    answers["_lc"] = answers["is_correct"] & (answers["part"] <= 4)
    answers["_rc"] = answers["is_correct"] & (answers["part"] >= 5)
    answers["_lq"] = answers["part"] <= 4
    answers["_rq"] = answers["part"] >= 5
    stats = answers.groupby("attempt_id").agg(
        lc=("_lc", "sum"), lq=("_lq", "sum"), rc=("_rc", "sum"), rq=("_rq", "sum"),
    ).reset_index()

    def to_score(rate):
        """Quy tỷ lệ đúng (0..1) về thang điểm TOEIC 5-495 (bội số của 5)."""
        return np.clip((np.round(rate * 99) * 5).astype(int), 5, 495)

    stats["listening_score"] = to_score((stats["lc"] / stats["lq"]).fillna(0))
    stats["reading_score"] = to_score((stats["rc"] / stats["rq"]).fillna(0))
    stats["total_score"] = stats["listening_score"] + stats["reading_score"]
    attempts = attempts.merge(
        stats[["attempt_id", "listening_score", "reading_score", "total_score"]],
        on="attempt_id", how="left",
    )
    answers = answers.drop(columns=["_lc", "_rc", "_lq", "_rq"])
    return attempts, answers


def inject_duplicates(answers, cfg, rng) -> pd.DataFrame:
    """Cài DUPLICATE: copy ~2% dòng, giữ nguyên (attempt_id, question_id), answer_id mới."""
    n_dup = int(len(answers) * cfg["duplicate_rate_offline"])
    dups = answers.iloc[rng.integers(0, len(answers), n_dup)].copy()
    dups["answer_id"] = np.arange(len(answers) + 1, len(answers) + 1 + n_dup)
    return pd.concat([answers, dups], ignore_index=True)


def write_single(df, out_dir, name):
    """Ghi 1 bảng ra 1 file Parquet: out_dir/name/data.parquet."""
    (out_dir / name).mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / name / "data.parquet", index=False)


def write_partitioned(df, out_dir, name, part_col, schema_change_date=None, drop_cols=None):
    """Ghi bảng partition theo part_col (Hive-style: part_col=VALUE/).

    Nếu có schema_change_date + drop_cols: partition ngày < schema_change_date sẽ
    BỎ HẲN các cột drop_cols khỏi file Parquet -> tạo schema evolution thật.
    """
    for val, sub in df.groupby(part_col):
        part_dir = out_dir / name / f"{part_col}={val}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out = sub.drop(columns=[part_col])
        if schema_change_date and drop_cols and str(val) < schema_change_date:
            out = out.drop(columns=[c for c in drop_cols if c in out.columns])
        out.to_parquet(part_dir / "data.parquet", index=False)


def main():
    """Chạy toàn bộ quy trình sinh dữ liệu offline và in tóm tắt."""
    cfg = load_config()
    rng = np.random.default_rng(cfg["random_seed"])
    out_dir = Path(cfg["output_dir"])
    if out_dir.exists():
        shutil.rmtree(out_dir)          # xóa dữ liệu cũ để chạy lại sạch
    out_dir.mkdir(parents=True, exist_ok=True)

    # schema_change_date nằm ở ~60% timeline -> có cả partition cũ (thiếu cột) lẫn mới
    schema_change = pd.Timestamp(cfg["schema_change_date"])
    start_date = schema_change - pd.Timedelta(days=int(0.6 * cfg["days_history"]))
    end_date = start_date + pd.Timedelta(days=cfg["days_history"])
    print(f"Sinh dữ liệu: {start_date.date()} -> {end_date.date()} "
          f"(schema_change={cfg['schema_change_date']}, seed={cfg['random_seed']})")

    users = gen_users(cfg, rng, start_date, end_date)
    questions = gen_questions(cfg, rng)
    vocab = gen_vocabulary(cfg, rng)
    grammar = gen_grammar_topics(cfg, rng)
    mock = gen_mock_tests(cfg, rng)
    attempts, answers = gen_attempts_and_answers(cfg, rng, users, questions, start_date)
    answers = inject_duplicates(answers, cfg, rng)

    # ---- Ghi Parquet ----
    write_partitioned(users, out_dir, "users", "signup_date")
    write_single(questions, out_dir, "questions")
    write_single(vocab, out_dir, "vocabulary")
    write_single(grammar, out_dir, "grammar_topics")
    write_single(mock, out_dir, "mock_tests")
    write_single(attempts, out_dir, "test_attempts")
    write_partitioned(answers, out_dir, "attempt_answers", "attempt_date",
                      schema_change_date=cfg["schema_change_date"],
                      drop_cols=["difficulty", "tag_grammar"])

    # ---- Tóm tắt ----
    print("Đã sinh xong:")
    print(f"  users            : {len(users):>10,}  (target 600-700 = "
          f"{users['target_score'].between(600, 700).mean():.1%})")
    print(f"  questions        : {len(questions):>10,}  (Part 5 = {(questions['part'] == 5).mean():.1%})")
    print(f"  vocabulary       : {len(vocab):>10,}")
    print(f"  grammar_topics   : {len(grammar):>10,}")
    print(f"  mock_tests       : {len(mock):>10,}")
    print(f"  test_attempts    : {len(attempts):>10,}")
    print(f"  attempt_answers  : {len(answers):>10,}  "
          f"(~{cfg['duplicate_rate_offline']:.0%} duplicate theo attempt_id+question_id)")
    print("Dữ liệu đã ghi vào:", out_dir.resolve())


if __name__ == "__main__":
    main()
