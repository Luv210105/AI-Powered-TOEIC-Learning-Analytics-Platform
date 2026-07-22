"""
main.py — điểm vào thông tin cho AI-Powered TOEIC Learning Analytics Platform.

In hướng dẫn nhanh; các thành phần thực tế nằm ở generator/, processing/, pipelines/, governance/.
Xem README.md để biết cấu trúc + cách chạy.
"""


def main() -> None:
    """In thông tin project + trỏ tới README."""
    print("AI-Powered TOEIC Learning Analytics Platform")
    print("Data platform: Generator -> Bronze/Silver/Gold -> Airflow -> DataHub")
    print("Xem README.md để biết cấu trúc và cách chạy (uv sync + docker compose up).")


if __name__ == "__main__":
    main()
