# app/services/analysis_service.py
import cx_Oracle
import pandas as pd
import plotly.graph_objects as go
import json

def analyze_product_sales(start_date, end_date, chart_type, db_connection_string):
    """
    주어진 기간(start_date ~ end_date)에 대한 상품 판매 분석을 수행한 뒤,
    Plotly 그래프의 data와 layout을 JSON으로 반환한다.

    :param start_date: str (YYYY-MM-DD)
    :param end_date: str (YYYY-MM-DD)
    :param chart_type: str ("line", "bar", "pie" 등)
    :param db_connection_string: str (ex: 'c##finalProject/1234@localhost:1521/xe')
    :return: (bool, dict) -> (성공여부, Plotly 그래프 JSON 또는 에러메시지)
    """
    try:
        print(f"[DEBUG] 분석 시작: start_date={start_date}, end_date={end_date}, chart_type={chart_type}")

        # 1) DB 연결
        print("[DEBUG] DB 연결 시도 중...")
        with cx_Oracle.connect(db_connection_string) as conn:
            print("[DEBUG] DB 연결 성공.")
            # 2) 쿼리
            print("[DEBUG] 쿼리 실행 중...")
            sql = """
            SELECT i.ITEM_NAME,
                   SUM(od.QUANTITY)    AS total_qty,
                   SUM(od.SUBTOTAL)    AS total_sales
            FROM ORDERS o
            JOIN ORDER_DETAIL od ON o.ORDER_NO = od.ORDER_NO
            JOIN ITEM i         ON i.ITEM_ID   = od.ITEM_ID
            WHERE o.ORDER_DATE BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') 
                                  AND TO_DATE(:end_date, 'YYYY-MM-DD')
            GROUP BY i.ITEM_NAME
            ORDER BY total_sales DESC
            """
            df = pd.read_sql(sql, conn, params={
                "start_date": start_date,
                "end_date": end_date
            })
            print(f"[DEBUG] 쿼리 결과: {len(df)} 행.")
            print(f"[DEBUG] DataFrame 컬럼: {df.columns.tolist()}")

        if df.empty:
            print("[DEBUG] 데이터프레임이 비어 있습니다.")
            return (False, {"error": "해당 기간에 대한 판매 데이터가 없습니다."})

        # 3) 데이터프레임 컬럼 소문자로 변환
        df.columns = [col.lower() for col in df.columns]
        print(f"[DEBUG] DataFrame 컬럼 (소문자 변환 후): {df.columns.tolist()}")

        # 4) 전처리 (필요 시 추가)
        print("[DEBUG] 데이터 전처리 시작...")
        # 예: 결측값 처리, 이상치 제거 등
        # 현재는 단순히 출력만
        print("[DEBUG] 데이터 전처리 완료.")

        # 5) 그래프 생성
        print("[DEBUG] 그래프 생성 시작...")
        fig = None
        if chart_type == "pie":
            print("[DEBUG] 파이 차트 생성 중...")
            fig = go.Figure(
                data=[go.Pie(labels=df["item_name"], values=df["total_sales"], hole=0.3)]
            )
            fig.update_layout(title_text="카테고리(또는 상품)별 매출 비중 (Pie)")

        elif chart_type == "bar":
            print("[DEBUG] 막대 그래프 생성 중...")
            fig = go.Figure(
                data=[go.Bar(x=df["item_name"], y=df["total_sales"])]
            )
            fig.update_layout(
                title="상품별 총 매출액(Bar)",
                xaxis_title="상품명",
                yaxis_title="매출액"
            )

        else:
            print("[DEBUG] 선 그래프 생성 중...")
            fig = go.Figure(
                data=[go.Scatter(x=df["item_name"], y=df["total_sales"], mode='lines+markers')]
            )
            fig.update_layout(
                title="상품별 총 매출액(Line)",
                xaxis_title="상품명",
                yaxis_title="매출액"
            )
        print("[DEBUG] 그래프 생성 완료.")

        # 6) Plotly 그래프의 data와 layout을 JSON으로 변환
        print("[DEBUG] Plotly 그래프를 JSON으로 변환 중...")
        graph_json = json.loads(fig.to_json())
        print("[DEBUG] Plotly 그래프 JSON 변환 완료.")

        return (True, graph_json)

    except Exception as e:
        print(f"[ERROR] 분석 중 예외 발생: {str(e)}")
        return (False, {"error": str(e)})
