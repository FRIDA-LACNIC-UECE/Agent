import requests
from app.main.config import app_config
from app.main.exceptions import DefaultException


def anonymize_new_rows(
    database_id: int, table_id: int, news_rows_client_db: list, token: str
):
    response = requests.post(
        url=f"{app_config.API_URL}/anonymization/database/{database_id}/table/{table_id}/database_rows",
        json={
            "rows_to_anonymization": news_rows_client_db,
            "insert_database": True,
        },
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("new_rows_not_anonymized", code=500)
