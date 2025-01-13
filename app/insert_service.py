import pandas as pd
import cx_Oracle

def process_and_insert_data(file_path, db_connection_string):
    """
    병합된 데이터를 Oracle DB에 삽입하는 로직.

    Args:
        file_path (str): 병합된 파일 경로
        db_connection_string (str): Oracle DB 연결 문자열

    Returns:
        bool, str: 삽입 성공 여부와 메시지
    """
    try:
        print(f"병합된 파일 읽는 중: {file_path}")
        merged_data = pd.read_excel(file_path)

        print("데이터 전처리 중...")
        # 'item_id'를 '상품코드'로 변경하고 'deliveryStatus'는 제외
        required_columns = [
            '유저번호', '년도', '월', '일',
            '매입매출구분(1-매출/2-매입)',
            '공급가액', '판매비와 관리비',
            '수량', '단가', '부가세',
            '품명', '과세유형', '상품코드'  # 'deliveryStatus' 제외
        ]

        # Excel 파일에 필수 컬럼이 모두 있는지 확인
        missing_columns = [col for col in required_columns if col not in merged_data.columns]
        if missing_columns:
            print(f"Excel 파일에 누락된 필수 컬럼: {missing_columns}")
            return False, f"Excel 파일에 누락된 필수 컬럼: {missing_columns}"

        # 결측치를 0으로 대체
        print("필수 컬럼의 결측값을 0으로 채우는 중...")
        merged_data[required_columns] = merged_data[required_columns].fillna(0)

        # 숫자형 컬럼 변환
        for col in ['공급가액', '판매비와 관리비', '수량', '단가', '부가세', '상품코드']:
            merged_data[col] = pd.to_numeric(merged_data[col], errors='coerce').fillna(0)

        # 날짜 유효성 검증 및 변환
        print("날짜 유효성 검증 및 변환 중...")
        # Create a 'order_date' column
        merged_data['order_date'] = pd.to_datetime(
            merged_data[['년도', '월', '일']].astype(int).astype(str).agg(''.join, axis=1),
            format='%Y%m%d',
            errors='coerce'
        )

        # Check for invalid dates
        invalid_dates = merged_data['order_date'].isnull()
        if invalid_dates.any():
            print(f"유효하지 않은 날짜가 {invalid_dates.sum()}개 발견되었습니다. 해당 행을 제거합니다.")
            filtered_data = merged_data[
                (merged_data['공급가액'] > 0) &
                (merged_data['판매비와 관리비'] >= 0) &
                (~invalid_dates)
            ]
        else:
            filtered_data = merged_data[
                (merged_data['공급가액'] > 0) &
                (merged_data['판매비와 관리비'] >= 0)
            ]

        print(f"필터링된 데이터 행 수: {len(filtered_data)}")

        if filtered_data.empty:
            print("삽입할 유효한 데이터가 없습니다. 데이터베이스 삽입을 건너뜁니다.")
            return False, "삽입할 유효한 데이터가 없습니다."

        # DataFrame 인덱스 리셋
        filtered_data = filtered_data.reset_index(drop=True)
        print("DataFrame 인덱스 리셋 완료.")

        # numpy 데이터 타입을 Python 기본 타입으로 변환
        print("numpy 데이터 타입을 Python 기본 타입으로 변환 중...")
        filtered_data = filtered_data.astype(object)

        print("Oracle DB에 연결 중...")
        connection = cx_Oracle.connect(db_connection_string)
        cursor = connection.cursor()

        # FINANCIAL_RECORD 테이블 삽입
        record_no_list = []
        financial_records = []
        for index, row in filtered_data.iterrows():
            cursor.execute("SELECT FINANCIAL_RECORD_SEQ.NEXTVAL FROM DUAL")
            record_no = cursor.fetchone()[0]
            record_no_list.append(record_no)

            financial_records.append((
                record_no,
                int(row['유저번호']),
                int(row['년도']),
                int(row['월']),
                int(row['일']),
                int(row['매입매출구분(1-매출/2-매입)']),
                int(row['과세유형']),
                float(row['수량']),
                float(row['단가']),
                float(row['공급가액']),
                float(row['부가세']),
                row['품명'],
                None  # item_id는 FINANCIAL_RECORD에 필요 없을 경우 None
            ))

        # FINANCIAL_RECORD 삽입을 executemany로 최적화
        print("FINANCIAL_RECORD 테이블 삽입 중...")
        cursor.executemany("""
            INSERT INTO FINANCIAL_RECORD (
                record_no, user_no, fin_year, fin_month, fin_day,
                transaction_type, tax_code, quantity, unit_price,
                supply_amount, vat, product_name, item_id
            ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)
        """, financial_records)

        # FINANCIAL_DETAILS 테이블 삽입
        try:
            sales_revenue = filtered_data[filtered_data["매입매출구분(1-매출/2-매입)"] == 1]["공급가액"].sum()
            cost_of_goods_sold = filtered_data[filtered_data["매입매출구분(1-매출/2-매입)"] == 2]["공급가액"].sum()
            sg_and_a_expenses = filtered_data["판매비와 관리비"].sum()
            net_income = sales_revenue - cost_of_goods_sold - sg_and_a_expenses

            cursor.execute("SELECT FINANCIAL_DETAILS_SEQ.NEXTVAL FROM DUAL")
            detail_no = cursor.fetchone()[0]

            details_data = (
                detail_no,
                record_no_list[-1] if record_no_list else None,
                float(sg_and_a_expenses),  # 판매비와 관리비 합계
                float(sales_revenue),      # 매출액
                float(cost_of_goods_sold), # 매출원가
                float(net_income)          # 순이익
            )

            print(f"FINANCIAL_DETAILS에 삽입 중: {details_data}")
            cursor.execute("""
                INSERT INTO FINANCIAL_DETAILS (
                    detail_no, record_no, sg_andAexpenses,
                    sales_revenue, cost_of_goods_sold, net_income
                ) VALUES (:1, :2, :3, :4, :5, :6)
            """, details_data)
        except Exception as details_error:
            print(f"FINANCIAL_DETAILS 삽입 중 에러 발생: {details_error}")
            raise

        # ORDER 테이블 삽입
        order_no_list = []
        orders = []
        for index, row in filtered_data.iterrows():
            cursor.execute("SELECT ORDER_SEQ.NEXTVAL FROM DUAL")
            order_no = cursor.fetchone()[0]
            order_no_list.append(order_no)

            # 주문 날짜는 이미 'order_date'로 변환되었음
            order_date_str = row['order_date'].strftime('%Y%m%d')  # 'YYYYMMDD'

            # 총 금액 계산 (공급가액 + 부가세)
            total_amount = float(row['공급가액']) + float(row['부가세'])

            # 배송 상태 (엑셀에 'deliveryStatus' 컬럼이 없으므로 기본값 사용)
            delivery_status = "배송 준비 중"

            orders.append((
                order_no,
                int(row['유저번호']),
                order_date_str,    # 'YYYYMMDD' 형식의 문자열
                total_amount,
                delivery_status
            ))

        # ORDER 삽입을 executemany로 최적화
        print("ORDER 테이블 삽입 중...")
        cursor.executemany("""
            INSERT INTO orders (
                order_no, user_no, order_date, total_amount,
                delivery_status
            ) VALUES (:1, :2, TO_DATE(:3, 'YYYYMMDD'), :4, :5)
        """, orders)

        # ORDER_DETAIL 테이블 삽입
        order_details = []
        for index, row in filtered_data.iterrows():
            cursor.execute("SELECT ORDER_DETAIL_SEQ.NEXTVAL FROM DUAL")
            order_detail_no = cursor.fetchone()[0]

            order_no = order_no_list[index]

            # item_id 매핑 (엑셀에 '상품코드' 컬럼을 'item_id'로 매핑)
            item_code = row.get('상품코드', None)
            if pd.isna(item_code) or item_code == 0:
                item_id = None
            else:
                item_id = int(item_code)

            # transactionType 값 설정 (NULL 방지)
            transaction_type = int(row.get('매입매출구분(1-매출/2-매입)', 0))  # 기본값 0으로 설정

            # transactionType 값 검증
            if transaction_type not in [1, 2]:  # 유효한 값은 1(매출) 또는 2(매입)
                print(f"유효하지 않은 transactionType: {transaction_type}. 기본값 0으로 설정.")
                transaction_type = 0

            order_details.append((
                order_detail_no,
                order_no,
                item_id,  # 실제 데이터 매핑 필요
                float(row['수량']),
                float(row['공급가액']),
                float(row['부가세']),
                transaction_type  # 매입/매출 구분
            ))

        # ORDER_DETAIL 삽입을 executemany로 최적화
        print("ORDER_DETAIL 테이블 삽입 중...")
        cursor.executemany("""
            INSERT INTO order_detail (
                order_detail_no, order_no, item_id, quantity,
                subtotal, vat, transaction_type
            ) VALUES (:1, :2, :3, :4, :5, :6, :7)
        """, order_details)

        # 모든 삽입이 성공적으로 완료되면 커밋
        connection.commit()
        print("데이터가 데이터베이스에 성공적으로 삽입되었습니다.")
        return True, "데이터가 데이터베이스에 성공적으로 삽입되었습니다."

    except Exception as e:
        print(f"process_and_insert_data에서 에러 발생: {e}")
        if 'connection' in locals() and connection:
            connection.rollback()  # 트랜잭션 롤백
        return False, str(e)

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection is not None:
            connection.close()
