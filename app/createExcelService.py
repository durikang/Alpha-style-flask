import pandas as pd
import io
from sqlalchemy import create_engine, text

# Oracle DB 연결 정보
DB_CONNECTION_STRING = "oracle+cx_oracle://c##finalProject:1234@localhost:1521/xe"

# 거래 유형 매핑
TRANSACTION_TYPE_MAPPING = {
    '매출': 1,
    '매입': 2
}

def generate_financial_excel_stream(start_date, end_date, transaction_type):
    """
    금융 기록 데이터를 Oracle DB에서 조회하고 엑셀 파일 스트리밍 생성
    """
    try:
        # SQLAlchemy를 통한 DB 연결
        engine = create_engine(DB_CONNECTION_STRING)
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
        WHERE TO_DATE(FIN_YEAR || '-' || FIN_MONTH || '-' || FIN_DAY, 'YYYY-MM-DD')
              BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
        """
        if transaction_type:
            query += " AND TRANSACTION_TYPE = :transaction_type"

        # 매개변수 설정
        params = {'start_date': start_date, 'end_date': end_date}
        if transaction_type in TRANSACTION_TYPE_MAPPING:
            params['transaction_type'] = TRANSACTION_TYPE_MAPPING[transaction_type]

        # 데이터 조회
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            data = result.fetchall()

            if not data:
                raise ValueError("조건에 맞는 데이터가 없습니다.")

            # DataFrame 생성
            df = pd.DataFrame(data, columns=result.keys())

        # 엑셀 데이터 스트리밍 생성
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Financial Records')
        buffer.seek(0)

        yield buffer.getvalue()

    except Exception as e:
        raise Exception(f"엑셀 생성 중 오류 발생: {e}")
