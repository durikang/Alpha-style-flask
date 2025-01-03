import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import json
import folium
import cx_Oracle


def process_all_analysis():
    try:

        # 1. 오라클 데이터베이스 연결 설정
        dsn = cx_Oracle.makedsn("your_host", 1521, service_name="your_service_name")
        connection = cx_Oracle.connect(user="c##finalProject", password="1234", dsn=dsn)

        # 2. 오라클 데이터 쿼리 실행 및 읽기
        oracle_query = "SELECT * FROM YOUR_TABLE"  # 여기에 실제 쿼리를 작성하세요
        cursor = connection.cursor()
        cursor.execute(oracle_query)

        # 오라클 데이터 -> Pandas DataFrame으로 변환
        columns = [col[0] for col in cursor.description]  # 컬럼 이름 가져오기
        data = cursor.fetchall()
        oracle_data = pd.DataFrame(data, columns=columns)

        # 오라클 연결 닫기
        cursor.close()
        connection.close()

        print("오라클 데이터 읽기 완료")

        # 3. 데이터 처리
        oracle_data.replace(['-'], np.nan, inplace=True)


        # 경로 설정
        input_file = './merged/merged_data.xlsx'
        user_file = './유저/가데이터.xlsx'
        output_dir = "./analysis"
        output_dir_html = "./analysis_html"
        geo_file_path = './유저/SIG.geojson'

        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(output_dir_html, exist_ok=True)

        # 엑셀 파일 읽기
        merged_data = pd.read_excel(input_file)
        user_data = pd.read_excel(user_file)
        merged_data.replace(['-'], np.nan, inplace=True)

        # ==================== 연도별 매출/판관비/순이익 ====================
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

        net_profit['매출'] /= 1e8
        net_profit['판관비'] /= 1e8
        net_profit['당기순이익'] /= 1e8

        # Excel 파일 저장
        sale_output = os.path.join(output_dir, "sale.xlsx")
        cost_output = os.path.join(output_dir, "cost.xlsx")
        net_profit_output = os.path.join(output_dir, "net_profit.xlsx")
        net_profit[['년도', '매출']].to_excel(sale_output, index=False)
        net_profit[['년도', '판관비']].to_excel(cost_output, index=False)
        net_profit[['년도', '당기순이익']].to_excel(net_profit_output, index=False)
        # 연도별 데이터를 개별 바 플롯으로 저장
        for year in net_profit['년도'].unique():
            year_data = net_profit[net_profit['년도'] == year]
            year_dir = os.path.join(output_dir_html, str(year))
            os.makedirs(year_dir, exist_ok=True)

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
            html_file = os.path.join(year_dir, f"{year}_재무상태표.html")
            fig.write_html(html_file)
            print(f"{year}년 HTML 파일이 성공적으로 저장되었습니다: {html_file}")

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

        # ==================== 연도별 성별 매출 비중 ====================
        sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1]
        merged_gender = pd.merge(sales_administrative, user_data, on='유저번호')
        merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')
        year_gender_spending = merged_gender.groupby(['년도', '성별'])['공급가액'].sum().reset_index()

        output_file_path = os.path.join(output_dir, "gender_spending.xlsx")

        # 병합된 데이터 저장
        year_gender_spending.to_excel(output_file_path, index=False)


        years = sorted(year_gender_spending['년도'].unique())
        male_data = year_gender_spending[year_gender_spending['성별'] == '남자'].set_index('년도')['공급가액']
        female_data = year_gender_spending[year_gender_spending['성별'] == '여자'].set_index('년도')['공급가액']

        # Excel 저장
        gender_output = os.path.join(output_dir, "gender_spending.xlsx")
        year_gender_spending.to_excel(gender_output, index=False)
        print(f"성별 매출 데이터 저장 완료: {gender_output}")

        # HTML 저장
        for year in sorted(year_gender_spending['년도'].unique()):
            year_data = year_gender_spending[year_gender_spending['년도'] == year]
            year_dir = os.path.join(output_dir_html, str(year))
            os.makedirs(year_dir, exist_ok=True)

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
            )
            html_file = os.path.join(year_dir, f"{year}_성별_매출.html")
            fig.write_html(html_file)
            print(f"{year}년 성별 매출 파이 차트 저장 완료: {html_file}")

        # Plotly 그래프 생성
        fig = go.Figure()

        # 남자 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=male_data,
            mode='lines+markers',
            name='남자 매출',
            line=dict(color='blue')
        ))

        # 여자 매출 데이터 추가
        fig.add_trace(go.Scatter(
            x=years,
            y=female_data,
            mode='lines+markers',
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

        print(f"병합된 데이터가 성공적으로 저장되었습니다: {output_file_path}")
        print(f"그래프 파일이 성공적으로 저장되었습니다: {html_file}")

#### 여기부터


        # ==================== 연도별 품목별 공급가액 ====================
        for year in sorted(net_profit['년도'].dropna().unique()):
            # 해당 연도의 데이터 필터링
            year_data = sales_data[sales_data['년도'] == year]

            # 연도별 디렉토리 생성
            year_dir = os.path.join(output_dir_html, str(year))
            os.makedirs(year_dir, exist_ok=True)

            # 품목별 공급가액 합계 정렬 (억 단위 변환)
            sales_price = (
                year_data.groupby('품명')['공급가액']
                .sum()
                .div(1e8)  # 억 단위로 변환
                .sort_values(ascending=False)
                .reset_index()
            )


            # 엑셀 파일 저장 경로
            output_file_path = os.path.join(output_dir, f"{year}_상품별_판매량.xlsx")
            sales_price.to_excel(output_file_path, index=False)

            # Plotly 바 플롯 생성
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=sales_price['품명'],
                        y=sales_price['공급가액'],
                        marker=dict(color='skyblue'),
                        text=sales_price['공급가액'].round(2),  # 텍스트 값은 소수점 2자리로 표시
                        textposition='auto',
                    )
                ]
            )

            # 레이아웃 설정
            fig.update_layout(
                title=f"{year}년 품명별 공급가액 합계",
                xaxis_title="품명",
                yaxis_title="공급가액 (단위: 억원)",  # 단위를 억 단위로 표시
                xaxis=dict(tickangle=45),  # X축 라벨 회전
                font=dict(family="Arial, sans-serif", size=12),
                margin=dict(l=50, r=50, t=50, b=100),  # 마진 설정
                yaxis=dict(tickformat=".1f"),  # Y축 레이블 소수점 한 자리로 표시
            )

            # HTML 파일로 저장
            html_file = os.path.join(year_dir, f"{year}_상품별_판매량.html")
            fig.write_html(html_file)

            print(f"{year}년 데이터가 저장되었습니다: {output_file_path}")
            print(f"{year}년 그래프가 저장되었습니다: {html_file}")
        # ==================== 품명별 공급가액 ====================
        # 기존 매출 데이터 `sales_data` 재활용
        sales_price = (
            sales_data.groupby('품명')['공급가액']
            .sum()
            .div(1e8)  # 억 단위로 변환
            .sort_values(ascending=False)
            .reset_index()
        )


        # 엑셀 파일로 저장
        sales_excel_path = os.path.join(output_dir, "상품별_판매량.xlsx")
        sales_price.to_excel(sales_excel_path, index=False)

        # Plotly 바 플롯 생성
        fig = go.Figure(
            data=[
                go.Bar(
                    x=sales_price['품명'],
                    y=sales_price['공급가액'],
                    marker=dict(color='skyblue'),
                    text=sales_price['공급가액'].round(2),  # 텍스트 값은 소수점 2자리로 표시
                    textposition='auto',
                )
            ]
        )

        # 그래프 레이아웃 설정
        fig.update_layout(
            title="품명별 공급가액 합계",
            xaxis_title="품명",
            yaxis_title="공급가액 (단위: 억원)",  # 단위를 억 단위로 표시
            xaxis=dict(tickangle=45),  # X축 라벨 회전
            font=dict(family="Arial, sans-serif", size=12),
            margin=dict(l=50, r=50, t=50, b=100),  # 여백 설정
            yaxis=dict(tickformat=".1f"),  # Y축 레이블 소수점 한 자리로 표시
        )

        # HTML 파일로 저장
        sales_html_path = os.path.join(output_dir_html, "연도별_상품별_판매량.html")
        fig.write_html(sales_html_path)


        print(f"품명별 공급가액 엑셀 파일이 저장되었습니다: {sales_excel_path}")
        print(f"품명별 공급가액 그래프 HTML 파일이 저장되었습니다: {sales_html_path}")

        for year in sorted(years):
            # 해당 연도의 데이터 필터링
            year_data = merged_gender[merged_gender['년도'] == year]
            year_dir = os.path.join(output_dir_html, str(year))
            os.makedirs(year_dir, exist_ok=True)


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

            # 엑셀 파일 저장 경로
            output_file_path = os.path.join(output_dir, f"{year}_VIP_유저.xlsx")
            sales_user_value_sorted.to_excel(output_file_path, index=False)

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
                margin=dict(l=50, r=50, t=50, b=100)
            )

            # HTML 파일 저장
            html_file_path = os.path.join(year_dir, f"{year}_VIP_유저.html")
            fig.write_html(html_file_path)

            print(f"{year}년 누적 금액 영역 그래프가 성공적으로 저장되었습니다: {html_file_path}")

        # ==================== 누적 금액 영역 그래프 (전체 유저 기준) ====================

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
            margin=dict(l=50, r=50, t=50, b=100)
        )

        # HTML 파일 저장
        html_file_path = os.path.join(output_dir_html, "전체_판매량_VIP.html")
        fig.write_html(html_file_path)


        print(f"누적 금액 영역 그래프가 성공적으로 저장되었습니다: {html_file_path}")


#### 여기부터



        # GeoJSON 데이터 로드
        geo_file_path = './유저/SIG.geojson'
        with open(geo_file_path, encoding='UTF-8') as f:
            geo = json.load(f)

        # 지역코드와 위도, 경도 정보 추출
        region_coordinates = {}
        for feature in geo['features']:
            sig_cd = feature['properties']['SIG_CD']
            coords = feature['geometry']['coordinates']
            if feature['geometry']['type'] == 'MultiPolygon':
                lon, lat = coords[0][0][0][0], coords[0][0][0][1]
            elif feature['geometry']['type'] == 'Polygon':
                lon, lat = coords[0][0][0], coords[0][0][1]
            region_coordinates[sig_cd] = (lat, lon)

        # Excel 데이터 로드
        excel_file_path = './유저/유저 머지.xlsx'
        merged_user_data = pd.read_excel(excel_file_path)

        # 데이터 준비
        merged_user_area = merged_user_data[['지역코드', '년도', '공급가액']]
        user_supply_sum = merged_user_area.groupby(['지역코드', '년도'])['공급가액'].sum().reset_index()

        # 연도별 버블 차트 생성
        for year in sorted(user_supply_sum['년도'].unique()):
            # 해당 연도의 데이터 필터링
            year_data = user_supply_sum[user_supply_sum['년도'] == year]

            # 연도별 디렉토리 생성
            year_dir = os.path.join(output_dir_html, str(year))
            os.makedirs(year_dir, exist_ok=True)
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

            html_file_path = os.path.join(year_dir, f'{year}_지역별_판매량.html')
            map_year.save(html_file_path)
            print(f"'{html_file_path}'에 저장 완료")
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
        return True, "모든 분석 작업이 완료되었습니다."

    except Exception as e:
        return False, str(e)


