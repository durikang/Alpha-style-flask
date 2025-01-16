import pandas as pd
import cx_Oracle
import io

# Oracle DB 연결 정보
DB_CONNECTION_STRING = "c##finalProject/1234@localhost:1521/xe"


def generate_financial_excel():
    """
    금융 기록 데이터를 Oracle DB에서 조회하고 엑셀 파일로 생성
    """
    try:
        # Oracle DB 연결
        connection = cx_Oracle.connect(DB_CONNECTION_STRING)
        query = """
        SELECT 
            FIN_YEAR AS "Year",
            FIN_MONTH AS "Month",
            FIN_DAY AS "Day",
            QUANTITY AS "Quantity",
            SUPPLY_AMOUNT AS "Supply Amount",
            TRANSACTION_TYPE AS "Transaction Type",
            UNIT_PRICE AS "Unit Price",
            VAT AS "VAT",
            PRODUCT_NAME AS "Product Name",
            USER_NO AS "User No",
            ITEM_ID AS "Item ID",
            TAX_CODE AS "Tax Code",
            RECORD_NO AS "Record No"
        FROM FINANCIAL_RECORD
        """

        # 데이터 조회
        df = pd.read_sql(query, con=connection)

        # DB 연결 종료
        connection.close()

        # 엑셀 파일 생성 (메모리 버퍼에 저장)
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Financial Records')
        excel_buffer.seek(0)

        return excel_buffer

    except cx_Oracle.DatabaseError as e:
        raise Exception(f"Oracle DB 에러 발생: {str(e)}")
    except Exception as e:
        raise Exception(f"엑셀 생성 중 에러 발생: {str(e)}")
