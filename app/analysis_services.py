import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import json
import folium
import cx_Oracle
from datetime import datetime


def process_all_analysis():
    try:

        dsn = cx_Oracle.makedsn("localhost", 1521, service_name="xe")
        connection = cx_Oracle.connect(user="c##finalProject", password="1234", dsn=dsn)

        # 1. MEMBERS 테이블 데이터 읽기 (2번 쿼리 먼저 실행)
        cursor = connection.cursor()
        oracle_query_1 = "SELECT BIRTH_DATE, USER_NO, ADDRESS, GENDER FROM MEMBERS"
        cursor.execute(oracle_query_1)

        # 첫 번째 쿼리 결과 처리
        columns_1 = [col[0] for col in cursor.description]
        data_1 = cursor.fetchall()
        oracle_data = pd.DataFrame(data_1, columns=columns_1)
        oracle_data.replace(['-'], np.nan, inplace=True)
        oracle_data.columns = ["나이", "유저번호", "지역", "성별"]

        # 2. ITEM 및 SUB_CATEGORY 테이블 데이터 읽기 (1번 쿼리 실행)
        oracle_query_2 = "SELECT I.ITEM_NAME, I.SUB_CATEGORY_ID, SC.SUB_CATEGORY_NAME FROM ITEM I JOIN SUB_CATEGORY SC ON I.SUB_CATEGORY_ID = SC.ID"
        cursor.execute(oracle_query_2)

        # 두 번째 쿼리 결과 처리
        columns_2 = [col[0] for col in cursor.description]
        data_2 = cursor.fetchall()

        oracle_item = pd.DataFrame(data_2, columns=columns_2)
        oracle_item.replace(['-'], np.nan, inplace=True)
        oracle_item.columns = ["품명", "카테고리 번호", "카테고리"]

        # 연결 닫기
        cursor.close()
        connection.close()

        # 현재 연도 가져오기
        current_year = datetime.now().year

        # "나이" 컬럼을 문자열로 변환 후 연도 추출
        oracle_data['나이'] = oracle_data['나이'].astype(str).str[:4].astype(int)
        oracle_data['나이'] = current_year - oracle_data['나이']  # 현재 연도에서 빼기

        # 경로 설정
        input_file = './merged/merged_data.xlsx'
        output_dir = "./analysis"
        output_dir_html = "./analysis_html"
        output_dir_png = "./analysis_png"

        geo_file_path = './유저/SIG.geojson'

        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(output_dir_html, exist_ok=True)

        # 엑셀 파일 읽기
        merged_data = pd.read_excel(input_file)
        merged_data.replace(['-'], np.nan, inplace=True)

        # ====== 연도별 매출/판관비/순이익 ======
        sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
        sales_data['년도'] = sales_data['년도'].astype(str).str.extract(r'(\d{4})')[0]
        sales_data['년도'] = pd.to_numeric(sales_data['년도'], errors='coerce')
        sales_data['수량'] = pd.to_numeric(sales_data['수량'], errors='coerce')
        sales_data['단가'] = pd.to_numeric(sales_data['단가'], errors='coerce')
        sales_data['매출'] = sales_data['수량'] * sales_data['단가']
        sales_by_year = sales_data.groupby('년도')['매출'].sum().reset_index()
        cost_data = merged_data[
            (merged_data['매입매출구분(1-매출/2-매입)'] == 2) |
            (merged_data['판매비와 관리비'].notna())
            ].copy()
        cost_data['년도'] = cost_data['년도'].astype(str).str.extract(r'(\d{4})')[0]
        cost_data['년도'] = pd.to_numeric(cost_data['년도'], errors='coerce')
        cost_data['판매비와 관리비'] = pd.to_numeric(cost_data['판매비와 관리비'], errors='coerce')
        cost_data['수량'] = pd.to_numeric(cost_data['수량'], errors='coerce')
        cost_data['단가'] = pd.to_numeric(cost_data['단가'], errors='coerce')
        cost_data['매출'] = cost_data['수량'] * cost_data['단가']
        cost_by_year = cost_data.groupby('년도').agg(
            매입_합계=('매출', 'sum'),
            판관비_합계=('판매비와 관리비', 'sum')
        ).reset_index()
        cost_by_year['판관비'] = cost_by_year['매입_합계'] + cost_by_year['판관비_합계']
        cost_by_year = cost_by_year[['년도', '판관비']]

        net_profit = pd.merge(sales_by_year, cost_by_year, how='left', on='년도')
        net_profit['당기순이익'] = net_profit['매출'] - net_profit['판관비']
        net_profit.fillna(0, inplace=True)



        # Excel 파일 저장
        sale_output = os.path.join(output_dir, "sale.xlsx")
        cost_output = os.path.join(output_dir, "cost.xlsx")
        net_profit_output = os.path.join(output_dir, "net_profit.xlsx")
        net_profit[['년도', '매출']].to_excel(sale_output, index=False)
        net_profit[['년도', '판관비']].to_excel(cost_output, index=False)
        net_profit[['년도', '당기순이익']].to_excel(net_profit_output, index=False)

        net_profit['매출'] /= 1e8
        net_profit['판관비'] /= 1e8
        net_profit['당기순이익'] /= 1e8
        # 연도별 데이터를 개별 바 플롯으로 저장
        for year in net_profit['년도'].unique():
            year_data = net_profit[net_profit['년도'] == year]
            year_dir_html = os.path.join(output_dir_html, str(year))
            year_dir_png = os.path.join(output_dir_png, str(year))
            os.makedirs(year_dir_html, exist_ok=True)
            os.makedirs(year_dir_png, exist_ok=True)

            # Plotly 바 플롯 생성
            fig = go.Figure()

            # 매출 바 추가
            fig.add_trace(go.Bar(x=[str(year)], y=year_data['매출'], name='매출', marker=dict(color='red')))

            # 판관비 바 추가
            fig.add_trace(go.Bar(x=[str(year)], y=year_data['판관비'], name='판관비', marker=dict(color='blue')))

            # 당기순이익 바 추가
            fig.add_trace(go.Bar(x=[str(year)], y=year_data['당기순이익'], name='당기순이익', marker=dict(color='green')))

            # 그래프 레이아웃 설정
            fig.update_layout(
                title=f"{year}년 매출, 판관비 및 당기순이익",
                xaxis_title="년도",
                yaxis_title="금액 (억 단위)",
                barmode='group',  # 그룹형 바 차트
                font=dict(family="Arial, sans-serif", size=12),
                legend=dict(orientation="h", y=-0.2),  # 범례 위치
                yaxis=dict(tickformat=".1f")  # Y축 포맷 설정 (소수점 한 자리)
            )

            # HTML로 저장
            html_file = os.path.join(year_dir_html, f"{year}_재무상태표.html")
            fig.write_html(html_file)
            print(f"{year}년 HTML 파일이 성공적으로 저장되었습니다: {html_file}")

            png_file = os.path.join(year_dir_png, f"{year}_재무상태표.png")
            # 해상도(가로/세로, 배율) 조절 가능
            fig.write_image(png_file, width=1800, height=1170)
            print(f"{year}년 PNG 파일이 성공적으로 저장되었습니다: {png_file}")

        # Plotly 꺾은선 그래프 생성
        fig = go.Figure()

        # 매출 꺾은선 추가
        fig.add_trace(go.Scatter(x=net_profit['년도'], y=net_profit['매출'], mode='lines+markers', name='매출',
                                 line=dict(color='red')))

        # 판관비 꺾은선 추가
        fig.add_trace(go.Scatter(x=net_profit['년도'], y=net_profit['판관비'], mode='lines+markers', name='판관비',
                                 line=dict(color='blue')))

        # 당기순이익 꺾은선 추가
        fig.add_trace(go.Scatter(x=net_profit['년도'], y=net_profit['당기순이익'], mode='lines+markers', name='당기순이익',
                                 line=dict(color='green')))

        # 그래프 레이아웃 설정
        fig.update_layout(
            title="연도별 매출, 판관비 및 당기순이익",
            xaxis_title="년도",
            yaxis_title="금액 (억 단위)",
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),
            yaxis=dict(tickformat=".1f"),
        )

        # HTML로 저장
        html_file = os.path.join(output_dir_html, "연도별_재무상태표.html")
        fig.write_html(html_file)
        # HTML 파일 저장
        fig = go.Figure()
        fig.add_trace(go.Bar(x=net_profit['년도'], y=net_profit['매출'], name='매출', marker=dict(color='red')))
        fig.add_trace(go.Bar(x=net_profit['년도'], y=net_profit['판관비'], name='판관비', marker=dict(color='blue')))
        fig.add_trace(go.Bar(x=net_profit['년도'], y=net_profit['당기순이익'], name='당기순이익', marker=dict(color='green')))
        fig.update_layout(
            title="연도별 매출, 판관비 및 순이익",
            xaxis_title="년도",
            yaxis_title="금액 (억 단위)",
            barmode='group',
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),
            yaxis=dict(tickformat=".1f")
        )
        html_file = os.path.join(output_dir_html, "연도별_매출_판관비_순이익.html")
        fig.write_html(html_file)
        print(f"매출/판관비/순이익 그래프 저장 완료: {html_file}")

        # PNG로 저장
        png_file = os.path.join(output_dir_png, "연도별_매출_판관비_순이익.png")
        fig.write_image(png_file, width=1800, height=1170)  # 해상도 설정
        print(f"그래프 파일이 성공적으로 PNG로 저장되었습니다: {png_file}")

        # ====== 연도별 나이 대 별 매출 비중 ======
        sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1]
        merged_age = pd.merge(sales_administrative, oracle_data, on='유저번호')
        merged_age['년도'] = pd.to_numeric(merged_age['년도'], errors='coerce')
        bins = [10, 20, 30, 40, 50]  # 경계값 설정
        labels = ['10대', '20대', '30대', '40대']  # 구간 이름
        merged_age['나이대'] = pd.cut(merged_age['나이'], bins=bins, labels=labels, right=False)

        year_age_spending = merged_age.groupby(['년도', '나이대'])['공급가액'].sum().reset_index()

        years = sorted(year_age_spending['년도'].unique())
        age_10_data = year_age_spending[year_age_spending['나이대'] == '10대'].set_index('년도')['공급가액']
        age_20_data = year_age_spending[year_age_spending['나이대'] == '20대'].set_index('년도')['공급가액']
        age_30_data = year_age_spending[year_age_spending['나이대'] == '30대'].set_index('년도')['공급가액']
        age_40_data = year_age_spending[year_age_spending['나이대'] == '40대'].set_index('년도')['공급가액']

        age_10_data /= 1e8
        age_20_data /= 1e8
        age_30_data /= 1e8
        age_40_data /= 1e8

        age_output = os.path.join(output_dir, "나이대별_판매량.xlsx")
        year_age_spending.to_excel(age_output, index=False)
        print(f"나이대별 매출 데이터 저장 완료: {age_output}")

        # HTML 저장
        for year in sorted(year_age_spending['년도'].unique()):
            year_data = year_age_spending[year_age_spending['년도'] == year]
            year_dir_html = os.path.join(output_dir_html, str(year))
            year_dir_png = os.path.join(output_dir_png, str(year))
            os.makedirs(year_dir_html, exist_ok=True)
            os.makedirs(year_dir_png, exist_ok=True)

            fig = go.Figure(data=[
                go.Pie(
                    labels=year_data['나이대'],
                    values=year_data['공급가액'],
                    hole=0.3,
                    textinfo='label+percent'
                )
            ])
            fig.update_layout(
                title=f"{year}년 나이대별 매출 비중",
                font=dict(family="Arial, sans-serif", size=12),
                legend=dict(
                    x=0,
                    y=1,
                    xanchor="left",
                    yanchor="top"
                )
            )
            html_file = os.path.join(year_dir_html, f"{year}_나이대별_매출.html")
            fig.write_html(html_file)
            print(f"{year}년 나이대별 매출 파이 차트 저장 완료: {html_file}")

            png_file = os.path.join(year_dir_png, f"{year}_나이대별_매출.png")
            # 해상도(가로/세로, 배율) 조절 가능
            fig.write_image(png_file, width=1800, height=1170)
            print(f"{year}년 PNG 파일이 성공적으로 저장되었습니다: {png_file}")

        # Plotly 그래프 생성
        fig = go.Figure()

        # 10대 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=age_10_data,
            mode='lines',  # 꺾은선 그래프로 설정
            name='10대 매출',
            line=dict(color='blue')
        ))

        # 20대 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=age_20_data,
            mode='lines',  # 꺾은선 그래프로 설정
            name='20대 매출',
            line=dict(color='red')
        ))

        # 30대 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=age_30_data,
            mode='lines',  # 꺾은선 그래프로 설정
            name='30대 매출',
            line=dict(color='green')
        ))

        # 40대 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=age_40_data,
            mode='lines',  # 꺾은선 그래프로 설정
            name='40대 매출',
            line=dict(color='yellow')
        ))

        # 그래프 레이아웃 설정
        fig.update_layout(
            title='연도별 나이대별 매출',
            xaxis_title='년도',
            yaxis_title='금액 (억 단위)',
            yaxis=dict(tickformat='.1f'),  # Y축 단위를 소수점으로 설정
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),  # 범례 위치
        )

        # HTML로 저장
        html_file = os.path.join(output_dir_html, "연도별_나이대별_매출.html")
        fig.write_html(html_file)

        print(f"그래프 파일이 성공적으로 저장되었습니다: {html_file}")

        # PNG로 저장
        png_file = os.path.join(output_dir_png, "연도별_나이대별_매출.png")
        fig.write_image(png_file, width=1800, height=1170)  # 해상도 설정
        print(f"그래프 파일이 성공적으로 PNG로 저장되었습니다: {png_file}")


        # ====== 연도별 성별 매출 비중 ======
        sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1]
        merged_gender = pd.merge(sales_administrative, oracle_data, on='유저번호')
        merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')
        year_gender_spending = merged_gender.groupby(['년도', '성별'])['공급가액'].sum().reset_index()



        years = sorted(year_gender_spending['년도'].unique())
        male_data = year_gender_spending[year_gender_spending['성별'] == '남'].set_index('년도')['공급가액']
        female_data = year_gender_spending[year_gender_spending['성별'] == '여'].set_index('년도')['공급가액']

        male_data /= 1e8
        female_data /= 1e8
        # Excel 저장
        gender_output = os.path.join(output_dir, "성별별_판매량.xlsx")
        year_gender_spending.to_excel(gender_output, index=False)
        print(f"성별 매출 데이터 저장 완료: {gender_output}")

        # HTML 저장
        for year in sorted(year_gender_spending['년도'].unique()):
            year_data = year_gender_spending[year_gender_spending['년도'] == year]
            year_dir_html = os.path.join(output_dir_html, str(year))
            year_dir_png = os.path.join(output_dir_png, str(year))
            os.makedirs(year_dir_html, exist_ok=True)
            os.makedirs(year_dir_png, exist_ok=True)

            fig = go.Figure(data=[
                go.Pie(
                    labels=year_data['성별'],
                    values=year_data['공급가액'],
                    hole=0.3,
                    textinfo='label+percent'
                )
            ])
            fig.update_layout(
                title=f"{year}년 성별 매출 비중",
                font=dict(family="Arial, sans-serif", size=12),
                legend=dict(
                    x=0,
                    y=1,
                    xanchor="left",
                    yanchor="top"
                )
            )
            html_file = os.path.join(year_dir_html, f"{year}_성별_매출.html")
            fig.write_html(html_file)
            print(f"{year}년 성별 매출 파이 차트 저장 완료: {html_file}")

            png_file = os.path.join(year_dir_png, f"{year}_성별_매출.png")
            # 해상도(가로/세로, 배율) 조절 가능
            fig.write_image(png_file, width=1800, height=1170)
            print(f"{year}년 PNG 파일이 성공적으로 저장되었습니다: {png_file}")

        # Plotly 그래프 생성
        fig = go.Figure()

        # 남자 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=male_data,
            mode='lines',  # 꺾은선 그래프로 설정
            name='남자 매출',
            line=dict(color='blue')
        ))

        # 여자 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=female_data,
            mode='lines',  # 꺾은선 그래프로 설정
            name='여자 매출',
            line=dict(color='red')
        ))

        # 그래프 레이아웃 설정
        fig.update_layout(
            title='연도별 성별 매출',
            xaxis_title='년도',
            yaxis_title='금액 (억 단위)',
            yaxis=dict(tickformat='.1f'),  # Y축 단위를 소수점으로 설정
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),  # 범례 위치
        )

        # HTML로 저장
        html_file = os.path.join(output_dir_html, "연도별_성별_매출.html")
        fig.write_html(html_file)

        print(f"병합된 데이터가 성공적으로 저장되었습니다: {gender_output}")
        print(f"그래프 파일이 성공적으로 저장되었습니다: {html_file}")

        # PNG로 저장
        png_file = os.path.join(output_dir_png, "연도별_성별_매출.png")
        fig.write_image(png_file, width=1800, height=1170)  # 해상도 설정
        print(f"그래프 파일이 성공적으로 PNG로 저장되었습니다: {png_file}")

        # ====== 연도별 품목별 공급가액 ======
        for year in sorted(net_profit['년도'].dropna().unique()):
            # 해당 연도의 데이터 필터링
            year_data = sales_data[sales_data['년도'] == year]
            # 연도별 디렉토리 생성
            year_dir_html = os.path.join(output_dir_html, str(year))
            year_dir_png = os.path.join(output_dir_png, str(year))
            os.makedirs(year_dir_html, exist_ok=True)
            os.makedirs(year_dir_png, exist_ok=True)




            # 품목별 공급가액 합계 정렬 (억 단위 변환)
            sales_price = (
                year_data.groupby('품명')['공급가액']
                .sum()
                .div(1e8)  # 억 단위로 변환
                .sort_values(ascending=False)
                .reset_index()
            )

            # 1. 품명 기준으로 데이터 병합
            sales_price = pd.merge(sales_price, oracle_item, on="품명", how="left")
            # 2. 카테고리 이름 기준으로 그룹화하여 공급가액 합계 계산
            sales_price = (
                sales_price.groupby("카테고리")["공급가액"]
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )

            excel_sales_price = (
                year_data.groupby('품명')['공급가액']
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )

            excel_sales_price = pd.merge(excel_sales_price, oracle_item, on="품명", how="left")

            excel_sales_price = (
                excel_sales_price.groupby("카테고리")["공급가액"]
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )


            # 엑셀 파일 저장 경로
            output_file_path = os.path.join(output_dir, f"{year}_카테고리별_판매량.xlsx")
            excel_sales_price.to_excel(output_file_path, index=False)

            # Plotly 바 플롯 생성
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=sales_price['카테고리'],
                        y=sales_price['공급가액'],
                        marker=dict(color='skyblue'),
                        text=sales_price['공급가액'].round(2),  # 텍스트 값은 소수점 2자리로 표시
                        textposition='auto',
                    )
                ]
            )


            # 레이아웃 설정
            fig.update_layout(
                title=f"{year}년 카테고리별 공급가액 합계",
                xaxis_title="카테고리",
                yaxis_title="공급가액 (단위: 억원)",  # 단위를 억 단위로 표시
                font=dict(family="Arial, sans-serif", size=12),
                margin=dict(l=50, r=50, t=50, b=100),  # 마진 설정
                yaxis=dict(tickformat=".1f"),  # Y축 레이블 소수점 한 자리로 표시
            )

            # HTML 파일로 저장
            html_file = os.path.join(year_dir_html, f"{year}_카테고리별_판매량.html")
            fig.write_html(html_file)

            print(f"{year}년 데이터가 저장되었습니다: {output_file_path}")
            print(f"{year}년 그래프가 저장되었습니다: {html_file}")

            png_file = os.path.join(year_dir_png, f"{year}_카테고리별_판매량.png")
            # 해상도(가로/세로, 배율) 조절 가능
            fig.write_image(png_file, width=1800, height=1170)
            print(f"{year}년 PNG 파일이 성공적으로 저장되었습니다: {png_file}")

        # ====== 품명별 공급가액 ======
        # 기존 매출 데이터 `sales_data` 재활용
        sales_price = (
            sales_data.groupby('품명')['공급가액']
            .sum()
            .div(1e8)  # 억 단위로 변환
            .sort_values(ascending=False)
            .reset_index()
        )
        # 1. 품명 기준으로 데이터 병합
        sales_price = pd.merge(sales_price, oracle_item, on="품명", how="left")

        # 2. 카테고리 이름 기준으로 그룹화하여 공급가액 합계 계산
        sales_price = (
            sales_price.groupby("카테고리")["공급가액"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        # 엑셀 파일로 저장
        sales_excel_path = os.path.join(output_dir, "카테고리별_판매량.xlsx")
        sales_price.to_excel(sales_excel_path, index=False)

        # Plotly 바 플롯 생성
        fig = go.Figure(
            data=[
                go.Bar(
                    x=sales_price['카테고리'],
                    y=sales_price['공급가액'],
                    marker=dict(color='skyblue'),
                    text=sales_price['공급가액'].round(2),  # 텍스트 값은 소수점 2자리로 표시
                    textposition='auto',
                )
            ]
        )

        # 그래프 레이아웃 설정
        fig.update_layout(
            title="카테고리별 공급가액 합계",
            xaxis_title="카테고리",
            yaxis_title="공급가액 (단위: 억원)",  # 단위를 억 단위로 표시
            font=dict(family="Arial, sans-serif", size=12),
            margin=dict(l=50, r=50, t=50, b=100),  # 여백 설정
            yaxis=dict(tickformat=".1f"),  # Y축 레이블 소수점 한 자리로 표시
        )

        # HTML 파일로 저장

        sales_html_path = os.path.join(output_dir_html, "연도별_카테고리별_판매량.html")
        fig.write_html(sales_html_path)

        print(f"품명별 공급가액 엑셀 파일이 저장되었습니다: {sales_excel_path}")
        print(f"품명별 공급가액 그래프 HTML 파일이 저장되었습니다: {sales_html_path}")

        # PNG로 저장
        png_file = os.path.join(output_dir_png, "연도별_카테고리별_판매량.png")
        fig.write_image(png_file, width=1800, height=1170)  # 해상도 설정
        print(f"그래프 파일이 성공적으로 PNG로 저장되었습니다: {png_file}")

        # 유저 데이터와 매출 데이터 병합
        merged_gender = pd.merge(sales_data, oracle_data, on='유저번호')
        merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')
        years = merged_gender['년도'].dropna().unique()

        for year in sorted(years):
            # 해당 연도의 데이터 필터링
            year_data = merged_gender[merged_gender['년도'] == year]
            year_dir_html = os.path.join(output_dir_html, str(year))
            year_dir_png = os.path.join(output_dir_png, str(year))
            os.makedirs(year_dir_html, exist_ok=True)
            os.makedirs(year_dir_png, exist_ok=True)

            # 그룹화 후 '공급가액'에 대한 합계 계산
            sales_user_quantity = (
                year_data.groupby('유저번호')['공급가액']
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )

            # 공급가액 기준 정렬 및 누적 금액 계산
            sales_user_value_sorted = sales_user_quantity.sort_values('공급가액', ascending=False)
            sales_user_value_sorted['누적금액'] = sales_user_value_sorted['공급가액'].cumsum()

            sales_user_value_sorted_xlxs = sales_user_value_sorted['누적금액']

            # 엑셀 파일 저장 경로
            output_file_path = os.path.join(output_dir, f"{year}_VIP_유저.xlsx")
            sales_user_value_sorted_xlxs.to_excel(output_file_path, index=False)

            # Y축의 최대값 계산 (억 단위로 변환)
            max_value = sales_user_value_sorted['누적금액'].max() / 1e8

            # x축을 비율로 설정
            x_vals = np.linspace(0, 1, len(sales_user_value_sorted))

            # Plotly 영역 그래프 생성
            fig = go.Figure()

            # 누적 금액 영역 그래프 추가
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=sales_user_value_sorted['누적금액'] / 1e8,  # 억 단위로 변환
                    fill='tozeroy',
                    mode='none',
                    fillcolor='skyblue',
                    name='누적 금액 (억 단위)'
                )
            )

            # 상위 10%, 20%, 30% 경계선 추가
            percentages = [0.1, 0.2, 0.3]
            cutoff_indices = [int(np.ceil(len(sales_user_value_sorted) * p)) for p in percentages]

            for cutoff_index, percent in zip(cutoff_indices, percentages):

                if cutoff_index > 0:
                    fig.add_trace(
                        go.Scatter(
                            x=[cutoff_index / len(sales_user_value_sorted),
                               cutoff_index / len(sales_user_value_sorted)],
                            y=[0, sales_user_value_sorted['누적금액'].iloc[cutoff_index - 1] / 1e8],
                            mode='lines',
                            line=dict(color='red', dash='dash'),
                            name=f'{int(percent * 100)}% 경계'
                        )
                    )

            # 그래프 레이아웃 설정
            fig.update_layout(
                title=f"{int(year)}년 상위 유저 소비 금액 누적 영역 그래프 (억 단위)",
                xaxis=dict(
                    title="유저 비율",
                    tickvals=np.linspace(0, 1, 11),
                    ticktext=[f"{int(i * 100)}%" for i in np.linspace(0, 1, 11)]
                ),
                yaxis=dict(
                    title="누적 금액 (억원)",
                    range=[0, max_value],  # Y축 범위 설정 (0에서 시작)
                    tickformat=".1f"
                ),
                font=dict(family="Arial, sans-serif", size=12),
                legend=dict(orientation="h", y=-0.2),  # 범례 위치
                margin=dict(l=50, r=50, t=50, b=100)
            )

            # HTML 파일 저장
            html_file_path = os.path.join(year_dir_html, f"{year}_VIP_유저.html")
            fig.write_html(html_file_path)

            print(f"{year}년 누적 금액 영역 그래프가 성공적으로 저장되었습니다: {html_file_path}")

            png_file = os.path.join(year_dir_png, f"{year}_VIP_유저.png")
            # 해상도(가로/세로, 배율) 조절 가능
            fig.write_image(png_file, width=1800, height=1170)
            print(f"{year}년 PNG 파일이 성공적으로 저장되었습니다: {png_file}")

        # ====== 누적 금액 영역 그래프 (전체 유저 기준) ======

        # 그룹화 후 '공급가액'에 대한 합계 계산
        sales_user_quantity = (
            merged_gender.groupby('유저번호')['공급가액']
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        # 엑셀 파일 저장 경로
        output_file_path = os.path.join(output_dir, f"{year}_VIP_유저.xlsx")
        sales_user_value_sorted.to_excel(output_file_path, index=False)

        # Y축의 최대값 계산 (억 단위로 변환)
        max_value = sales_user_value_sorted['누적금액'].max() / 1e8

        # 상위 10%, 20%, 30% 비율에 해당하는 경계값 찾기
        percentages = [0.1, 0.2, 0.3]
        cutoff_indices = [int(np.ceil(len(sales_user_value_sorted) * p)) for p in percentages]

        # x축을 비율로 설정
        x_vals = np.linspace(0, 1, len(sales_user_value_sorted))

        # Plotly 영역 그래프 생성
        fig = go.Figure()

        # 누적 금액 영역 그래프 추가
        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=sales_user_value_sorted['누적금액'] / 1e8,  # 억 단위로 변환
                fill='tozeroy',
                mode='none',
                fillcolor='skyblue',
                name='누적 금액 (억 단위)'
            )
        )

        # 상위 10%, 20%, 30% 경계선 추가
        for cutoff_index, percent in zip(cutoff_indices, percentages):
            fig.add_trace(
                go.Scatter(
                    x=[cutoff_index / len(sales_user_value_sorted), cutoff_index / len(sales_user_value_sorted)],
                    y=[0, sales_user_value_sorted['누적금액'].iloc[cutoff_index - 1] / 1e8],
                    mode='lines',
                    line=dict(color='red', dash='dash'),
                    name=f'{int(percent * 100)}% 경계'
                )
            )

        # 그래프 레이아웃 설정
        fig.update_layout(
            title="상위 유저 소비 금액 누적 영역 그래프 (억 단위)",
            xaxis=dict(
                title="유저 비율",
                tickvals=np.linspace(0, 1, 11),
                ticktext=[f"{int(i * 100)}%" for i in np.linspace(0, 1, 11)]
            ),
            yaxis=dict(
                title="누적 금액 (억원)",
                range=[0, max_value],  # Y축 범위 설정 (0에서 시작)
                tickformat=".1f"
            ),
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),  # 범례 위치
            margin=dict(l=50, r=50, t=50, b=100)
        )

        # HTML 파일 저장
        html_file_path = os.path.join(output_dir_html, "전체_판매량_VIP.html")
        fig.write_html(html_file_path)

        print(f"누적 금액 영역 그래프가 성공적으로 저장되었습니다: {html_file_path}")

        # PNG로 저장
        png_file = os.path.join(output_dir_png, "전체_판매량_VIP.png")
        fig.write_image(png_file, width=1800, height=1170)  # 해상도 설정
        print(f"그래프 파일이 성공적으로 PNG로 저장되었습니다: {png_file}")


        #### 여기부터

        # GeoJSON 데이터 로드
        with open(geo_file_path, encoding='UTF-8') as f:
            geo = json.load(f)


        # 지역코드와 위도, 경도 정보 추출
        region_data = {
            "서울특별시": 11,
            "서울특별시 종로구": 11110,
            "서울특별시 중구": 11140,
            "서울특별시 용산구": 11170,
            "서울특별시 성동구": 11200,
            "서울특별시 광진구": 11215,
            "서울특별시 동대문구": 11230,
            "서울특별시 중랑구": 11260,
            "서울특별시 성북구": 11290,
            "서울특별시 강북구": 11305,
            "서울특별시 도봉구": 11320,
            "서울특별시 노원구": 11350,
            "서울특별시 은평구": 11380,
            "서울특별시 서대문구": 11410,
            "서울특별시 마포구": 11440,
            "서울특별시 양천구": 11470,
            "서울특별시 강서구": 11500,
            "서울특별시 구로구": 11530,
            "서울특별시 금천구": 11545,
            "서울특별시 영등포구": 11560,
            "서울특별시 동작구": 11590,
            "서울특별시 관악구": 11620,
            "서울특별시 서초구": 11650,
            "서울특별시 강남구": 11680,
            "서울특별시 송파구": 11710,
            "서울특별시 강동구": 11740,
            "부산광역시": 26,
            "부산 중구": 26110,
            "부산 서구": 26140,
            "부산 동구": 26170,
            "부산 영도구": 26200,
            "부산진구": 26230,
            "부산 동래구": 26260,
            "부산 남구": 26290,
            "부산 북구": 26320,
            "부산 해운대구": 26350,
            "부산 사하구": 26380,
            "부산 금정구": 26410,
            "부산 강서구": 26440,
            "부산 연제구": 26470,
            "부산 수영구": 26500,
            "부산 사상구": 26530,
            "부산 기장군": 26710,
            "대구광역시": 27,
            "대구 중구": 27110,
            "대구 동구": 27140,
            "대구 서구": 27170,
            "대구 남구": 27200,
            "대구 북구": 27230,
            "대구 수성구": 27260,
            "대구 달서구": 27290,
            "대구 달성군": 27710,
            "인천광역시": 28,
            "인천 중구": 28110,
            "인천 동구": 28140,
            "인천 미추홀구": 28177,
            "인천 연수구": 28185,
            "인천 남동구": 28200,
            "인천 부평구": 28237,
            "인천 계양구": 28245,
            "인천 서구": 28260,
            "인천 강화군": 28710,
            "인천 옹진군": 28720,
            "광주광역시": 29,
            "광주 동구": 29110,
            "광주 서구": 29140,
            "광주 남구": 29155,
            "광주 북구": 29170,
            "광주 광산구": 29200,
            "대전광역시": 30,
            "대전 동구": 30110,
            "대전 중구": 30140,
            "대전 서구": 30170,
            "대전 유성구": 30200,
            "대전 대덕구": 30230,
            "울산광역시": 31,
            "울산 중구": 31110,
            "울산 남구": 31140,
            "울산 동구": 31170,
            "울산 북구": 31200,
            "울산 울주군": 31710,
            "세종특별자치시": 36,
            "세종시": 36110,
            "경기도": 41,
            "수원시": 41110,
            "수원시 장안구": 41111,
            "수원시 권선구": 41113,
            "수원시 팔달구": 41115,
            "수원시 영통구": 41117,
            "성남시": 41130,
            "성남시 수정구": 41131,
            "성남시 중원구": 41133,
            "성남시 분당구": 41135,
            "의정부시": 41150,
            "안양시": 41170,
            "안양시 만안구": 41171,
            "안양시 동안구": 41173,
            "부천시": 41190,
            "광명시": 41210,
            "평택시": 41220,
            "동두천시": 41250,
            "안산시": 41270,
            "안산시 상록구": 41271,
            "안산시 단원구": 41273,
            "고양시": 41280,
            "고양시 덕양구": 41281,
            "고양시 일산동구": 41285,
            "고양시 일산서구": 41287,
            "과천시": 41290,
            "구리시": 41310,
            "남양주시": 41360,
            "오산시": 41370,
            "시흥시": 41390,
            "군포시": 41410,
            "의왕시": 41430,
            "하남시": 41450,
            "용인시": 41460,
            "용인시 처인구": 41461,
            "용인시 기흥구": 41463,
            "용인시 수지구": 41465,
            "파주시": 41480,
            "이천시": 41500,
            "안성시": 41550,
            "김포시": 41570,
            "화성시": 41590,
            "광주시": 41610,
            "양주시": 41630,
            "포천시": 41650,
            "여주시": 41670,
            "연천군": 41800,
            "가평군": 41820,
            "양평군": 41830,
            "강원도": 42,
            "춘천시": 42110,
            "원주시": 42130,
            "강릉시": 42150,
            "동해시": 42170,
            "태백시": 42190,
            "속초시": 42210,
            "삼척시": 42230,
            "홍천군": 42720,
            "횡성군": 42730,
            "영월군": 42750,
            "평창군": 42760,
            "정선군": 42770,
            "철원군": 42780,
            "화천군": 42790,
            "양구군": 42800,
            "인제군": 42810,
            "고성군": 42820,
            "양양군": 42830,
            "충청북도": 43,
            "청주시": 43110,
            "청주시 서원구": 43112,
            "청주시 청원구": 43114,
            "청주시 상당구": 43111,
            "청주시 흥덕구": 43113,
            "충주시": 43130,
            "제천시": 43150,
            "보은군": 43720,
            "옥천군": 43730,
            "영동군": 43740,
            "증평군": 43745,
            "진천군": 43750,
            "괴산군": 43760,
            "음성군": 43770,
            "단양군": 43800,
            "충청남도": 44,
            "당진시": 44270,
            "천안시": 44130,
            "천안시 동남구": 44131,
            "천안시 서북구": 44133,
            "공주시": 44150,
            "보령시": 44180,
            "아산시": 44200,
            "서산시": 44210,
            "논산시": 44230,
            "계룡시": 44250,
            "금산군": 44710,
            "부여군": 44760,
            "서천군": 44770,
            "청양군": 44790,
            "홍성군": 44800,
            "예산군": 44810,
            "태안군": 44825,
            "전라북도": 45,
            "전주시": 45110,
            "전주시 완산구": 45111,
            "전주시 덕진구": 45113,
            "군산시": 45130,
            "익산시": 45140,
            "정읍시": 45180,
            "남원시": 45190,
            "김제시": 45210,
            "완주군": 45710,
            "진안군": 45720,
            "무주군": 45730,
            "장수군": 45740,
            "임실군": 45750,
            "순창군": 45770,
            "고창군": 45790,
            "부안군": 45800,
            "전라남도": 46,
            "목포시": 46110,
            "여수시": 46130,
            "순천시": 46150,
            "나주시": 46170,
            "광양시": 46230,
            "담양군": 46710,
            "곡성군": 46720,
            "구례군": 46730,
            "고흥군": 46770,
            "보성군": 46780,
            "화순군": 46790,
            "장흥군": 46800,
            "강진군": 46810,
            "해남군": 46820,
            "영암군": 46830,
            "무안군": 46840,
            "함평군": 46860,
            "영광군": 46870,
            "장성군": 46880,
            "완도군": 46890,
            "진도군": 46900,
            "신안군": 46910,
            "경상북도": 47,
            "포항시": 47110,
            "포항시 남구": 47111,
            "포항시 북구": 47113,
            "경주시": 47130,
            "김천시": 47150,
            "안동시": 47170,
            "구미시": 47190,
            "영주시": 47210,
            "영천시": 47230,
            "상주시": 47250,
            "문경시": 47280,
            "경산시": 47290,
            "군위군": 47720,
            "의성군": 47730,
            "청송군": 47750,
            "영양군": 47760,
            "영덕군": 47770,
            "청도군": 47820,
            "고령군": 47830,
            "성주군": 47840,
            "칠곡군": 47850,
            "예천군": 47900,
            "봉화군": 47920,
            "울진군": 47930,
            "울릉군": 47940,
            "경상남도": 48,
            "창원시": 48120,
            "창원시 의창구": 48121,
            "창원시 성산구": 48123,
            "창원시 마산합포구": 48125,
            "창원시 마산회원구": 48127,
            "창원시 진해구": 48129,
            "진주시": 48170,
            "통영시": 48220,
            "사천시": 48240,
            "김해시": 48250,
            "밀양시": 48270,
            "거제시": 48310,
            "양산시": 48330,
            "의령군": 48720,
            "함안군": 48730,
            "창녕군": 48740,
            "고성군": 48820,
            "남해군": 48840,
            "하동군": 48850,
            "산청군": 48860,
            "함양군": 48870,
            "거창군": 48880,
            "합천군": 48890,
            "제주특별자치도": 50,
            "제주시": 50110,
            "서귀포시": 50130
        }
        region_coordinates = {}
        pd.set_option("display.max_columns", None)  # 모든 열 출력
        pd.set_option("display.max_rows", None)  # 모든 행 출력
        pd.set_option("display.expand_frame_repr", False)  # 열이 화면 너비에 맞춰 표시되도록 설정

        oracle_data['지역코드'] = oracle_data['지역'].map(region_data)
        oracle_data['지역코드'] = oracle_data['지역코드'].astype('Int64')  # Pandas Nullable Integer
        sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
        merged_user_data = pd.merge(oracle_data, sales_data, on='유저번호')
        for feature in geo['features']:
            sig_cd = feature['properties']['SIG_CD']
            coords = feature['geometry']['coordinates']
            if feature['geometry']['type'] == 'MultiPolygon':
                lon, lat = coords[0][0][0][0], coords[0][0][0][1]
            elif feature['geometry']['type'] == 'Polygon':
                lon, lat = coords[0][0][0], coords[0][0][1]
            region_coordinates[sig_cd] = (lat, lon)



        # 데이터 준비
        merged_user_area = merged_user_data[['지역코드', '년도', '공급가액']]
        user_supply_sum = merged_user_area.groupby(['지역코드', '년도'])['공급가액'].sum().reset_index()

        # 연도별 버블 차트 생성
        for year in sorted(user_supply_sum['년도'].unique()):
            # 해당 연도의 데이터 필터링
            year_data = user_supply_sum[user_supply_sum['년도'] == year]

            # 연도별 디렉토리 생성
            year_dir_html = os.path.join(output_dir_html, str(year))
            year_dir_png = os.path.join(output_dir_png, str(year))
            os.makedirs(year_dir_html, exist_ok=True)
            os.makedirs(year_dir_png, exist_ok=True)

            map_center = [35.96, 127.1]
            map_year = folium.Map(location=map_center, zoom_start=8, tiles='cartodbpositron')

            for _, row in year_data.iterrows():
                region_code = str(row['지역코드'])
                supply_value = row['공급가액']
                if region_code in region_coordinates:
                    lat, lon = region_coordinates[region_code]
                    bubble_size = supply_value / 5e5
                    folium.CircleMarker(
                        location=[lat, lon],
                        radius=bubble_size,
                        fill=True,
                        fill_color='skyblue',
                        fill_opacity=0.6,
                        stroke=False,
                        popup=f'지역 코드: {region_code}<br>공급가액: {supply_value:,.0f}원'
                    ).add_to(map_year)

            html_file_path = os.path.join(year_dir_html, f'{year}_지역별_판매량.html')
            map_year.save(html_file_path)
            print(f"'{html_file_path}'에 저장 완료")

            png_file = os.path.join(year_dir_png, f"{year}_지역별_판매량.png")
            # 해상도(가로/세로, 배율) 조절 가능
            fig.write_image(png_file, width=1800, height=1170)
            print(f"{year}년 PNG 파일이 성공적으로 저장되었습니다: {png_file}")

        # 전체 데이터를 기반으로 한 버블 차트 생성
        user_supply_sum_total = merged_user_area.groupby(['지역코드'])['공급가액'].sum().reset_index()

        combined_map = folium.Map(location=[35.96, 127.1], zoom_start=8, tiles='cartodbpositron')
        for _, row in user_supply_sum_total.iterrows():
            region_code = str(row['지역코드'])
            supply_value = row['공급가액']
            if region_code in region_coordinates:
                lat, lon = region_coordinates[region_code]
                bubble_size = supply_value / 5e6
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=bubble_size,
                    fill=True,
                    fill_color='skyblue',
                    fill_opacity=0.6,
                    stroke=False,
                    popup=f'지역 코드: {region_code}<br>공급가액: {supply_value:,.0f}원'
                ).add_to(combined_map)

        html_file_path = os.path.join(output_dir_html, "연도별_지역별_판매량.html")
        combined_map.save(html_file_path)
        print(f"'{html_file_path}'에 저장 완료")

        print(f"누적 금액 영역 그래프가 성공적으로 저장되었습니다: {html_file_path}")

        # PNG로 저장
        png_file = os.path.join(output_dir_png, "연도별_지역별_판매량.png")
        fig.write_image(png_file, width=1800, height=1170)  # 해상도 설정
        print(f"그래프 파일이 성공적으로 PNG로 저장되었습니다: {png_file}")

        return True, "모든 분석 작업이 완료되었습니다."

    except Exception as e:
        return False, str(e)
