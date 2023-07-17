import requests
from app.main.config import app_config
from app.main.exceptions import DefaultException


def encrypt_new_rows(
    database_id: int, table_id: int, news_rows_client_db: list, token: str
):
    response = requests.post(
        url=f"{app_config.API_URL}/encryption/database/{database_id}/table/{table_id}/database_rows",
        json={
            "rows_to_encrypt": news_rows_client_db,
            "update_database": False,
        },
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("new_rows_not_encrypted", code=500)
