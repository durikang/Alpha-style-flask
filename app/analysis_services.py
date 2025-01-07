import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import json
import folium
import cx_Oracle
from datetime import datetime

# ----------------------------
# Utility Functions
# ----------------------------

def create_directories(output_dir, output_dir_html, output_dir_png):
    """
    Create necessary output directories if they do not exist.
    """
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_html, exist_ok=True)
    os.makedirs(output_dir_png, exist_ok=True)

def save_excel(dataframe, path):
    """
    Save a pandas DataFrame to an Excel file.
    """
    dataframe.to_excel(path, index=False)
    print(f"엑셀 파일이 저장되었습니다: {path}")

def save_plotly_fig(fig, html_path, png_path, width=1800, height=1170):
    """
    Save a Plotly figure as both HTML and PNG.
    """
    fig.write_html(html_path)
    print(f"HTML 파일이 저장되었습니다: {html_path}")
    fig.write_image(png_path, width=width, height=height)
    print(f"PNG 파일이 저장되었습니다: {png_path}")

def load_geojson(geo_file_path):
    """
    Load GeoJSON data from a file.
    """
    with open(geo_file_path, encoding='UTF-8') as f:
        return json.load(f)

def map_region_coordinates(geo):
    """
    Map region codes to their corresponding latitude and longitude.
    """
    region_coordinates = {}
    for feature in geo['features']:
        sig_cd = feature['properties']['SIG_CD']
        coords = feature['geometry']['coordinates']
        if feature['geometry']['type'] == 'MultiPolygon':
            lon, lat = coords[0][0][0][0], coords[0][0][0][1]
        elif feature['geometry']['type'] == 'Polygon':
            lon, lat = coords[0][0][0], coords[0][0][1]
        region_coordinates[sig_cd] = (lat, lon)
    return region_coordinates

# ----------------------------
# Data Retrieval and Preprocessing
# ----------------------------

def retrieve_oracle_data():
    """
    Connect to the Oracle database and retrieve MEMBERS and ITEM data.
    """
    try:
        dsn = cx_Oracle.makedsn("localhost", 1521, service_name="xe")
        connection = cx_Oracle.connect(user="c##finalProject", password="1234", dsn=dsn)
        cursor = connection.cursor()

        # Retrieve MEMBERS data
        oracle_query_1 = "SELECT BIRTH_DATE, USER_NO, ADDRESS, GENDER FROM MEMBERS"
        cursor.execute(oracle_query_1)
        columns_1 = [col[0] for col in cursor.description]
        data_1 = cursor.fetchall()
        oracle_data = pd.DataFrame(data_1, columns=columns_1)
        oracle_data.replace(['-'], np.nan, inplace=True)
        oracle_data.columns = ["나이", "유저번호", "지역", "성별"]

        # Retrieve ITEM and SUB_CATEGORY data
        oracle_query_2 = """
            SELECT I.ITEM_NAME, I.SUB_CATEGORY_ID, SC.SUB_CATEGORY_NAME 
            FROM ITEM I 
            JOIN SUB_CATEGORY SC ON I.SUB_CATEGORY_ID = SC.ID
        """
        cursor.execute(oracle_query_2)
        columns_2 = [col[0] for col in cursor.description]
        data_2 = cursor.fetchall()
        oracle_item = pd.DataFrame(data_2, columns=columns_2)
        oracle_item.replace(['-'], np.nan, inplace=True)
        oracle_item.columns = ["품명", "카테고리 번호", "카테고리"]

        cursor.close()
        connection.close()

        # Process Age
        current_year = datetime.now().year
        oracle_data['나이'] = oracle_data['나이'].astype(str).str[:4].astype(int)
        oracle_data['나이'] = current_year - oracle_data['나이']

        return oracle_data, oracle_item

    except Exception as e:
        print(f"데이터베이스 연결 또는 데이터 조회 중 오류 발생: {e}")
        raise

def load_merged_data(input_file):
    """
    Load merged data from an Excel file.
    """
    merged_data = pd.read_excel(input_file)
    merged_data.replace(['-'], np.nan, inplace=True)
    return merged_data

# ----------------------------
# Financial Metrics Calculation
# ----------------------------

def calculate_sales(merged_data):
    """
    Calculate total sales by year.
    """
    sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
    sales_data['년도'] = sales_data['년도'].astype(str).str.extract(r'(\d{4})')[0].astype(float)
    sales_data['공급가액'] = pd.to_numeric(sales_data['공급가액'], errors='coerce')
    sales_by_year = sales_data.groupby('년도')['공급가액'].sum().reset_index()
    sales_by_year.rename(columns={'공급가액': '매출'}, inplace=True)
    return sales_by_year, sales_data

def calculate_cost(merged_data):
    """
    Calculate total costs by year.
    """
    cost_data = merged_data[
        (merged_data['매입매출구분(1-매출/2-매입)'] == 2) |
        (merged_data['판매비와 관리비'].notna())
    ].copy()
    cost_data['년도'] = cost_data['년도'].astype(str).str.extract(r'(\d{4})')[0].astype(float)
    cost_data['판매비와 관리비'] = pd.to_numeric(cost_data['판매비와 관리비'], errors='coerce')
    cost_data['공급가액'] = pd.to_numeric(cost_data['공급가액'], errors='coerce')

    cost_by_year = cost_data.groupby('년도').agg(
        매입_합계=('공급가액', 'sum'),
        판관비_합계=('판매비와 관리비', 'sum')
    ).reset_index()
    cost_by_year['판관비'] = cost_by_year['매입_합계'] + cost_by_year['판관비_합계']
    cost_by_year = cost_by_year[['년도', '판관비']]
    return cost_by_year


def calculate_net_profit(sales_by_year, cost_by_year):
    """
    Calculate net profit by merging sales and cost data.
    """
    net_profit = pd.merge(sales_by_year, cost_by_year, how='left', on='년도')
    net_profit['당기순이익'] = net_profit['매출'] - net_profit['판관비']
    net_profit.fillna(0, inplace=True)
    data_net_profit = net_profit.copy()  # 원본 데이터를 유지

    print(data_net_profit)  # 디버깅용 출력

    # Convert to 억 단위
    net_profit[['매출', '판관비', '당기순이익']] /= 1e8

    return net_profit, data_net_profit


def save_financial_metrics(data_net_profit, output_dir):
    """
    Save financial metrics (sales, costs, net profit) to Excel files.
    """
    # 파일 저장 경로
    sale_output = os.path.join(output_dir, "sale.xlsx")
    cost_output = os.path.join(output_dir, "cost.xlsx")
    net_profit_output = os.path.join(output_dir, "net_profit.xlsx")

    # 억 단위 변환 전 데이터 저장
    data_net_profit[['년도', '매출']].to_excel(sale_output, index=False)
    data_net_profit[['년도', '판관비']].to_excel(cost_output, index=False)
    data_net_profit[['년도', '당기순이익']].to_excel(net_profit_output, index=False)

    print(f"Financial metrics Excel files saved: {sale_output}, {cost_output}, {net_profit_output}")

def plot_financial_data(net_profit, output_dir_html, output_dir_png):
    """
    Generate plots for financial data (sales, costs, net profit) by year.
    """
    # Ensure years are integers
    net_profit['년도'] = net_profit['년도'].astype(int)

    # Plot per year
    for year in net_profit['년도'].unique():
        year_data = net_profit[net_profit['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)

        # Plotly bar plot
        fig = go.Figure()
        fig.add_trace(go.Bar(x=[str(year)], y=year_data['매출'], name='매출', marker=dict(color='red')))
        fig.add_trace(go.Bar(x=[str(year)], y=year_data['판관비'], name='판관비', marker=dict(color='blue')))
        fig.add_trace(go.Bar(x=[str(year)], y=year_data['당기순이익'], name='당기순이익', marker=dict(color='green')))
        fig.update_layout(
            title=f"{year}년 매출, 판관비 및 당기순이익",
            xaxis_title="년도",
            yaxis_title="금액 (억 단위)",
            barmode='group',
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),
            yaxis=dict(tickformat=".1f")
        )

        # Save plots
        html_file = os.path.join(year_dir_html, f"{year}_재무상태표.html")
        fig.write_html(html_file)
        png_file = os.path.join(year_dir_png, f"{year}_재무상태표.png")
        fig.write_image(png_file, width=1800, height=1170)

    # Plot for all years
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=net_profit['년도'], y=net_profit['매출'], mode='lines+markers', name='매출', line=dict(color='red')))
    fig.add_trace(go.Scatter(x=net_profit['년도'], y=net_profit['판관비'], mode='lines+markers', name='판관비', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=net_profit['년도'], y=net_profit['당기순이익'], mode='lines+markers', name='당기순이익', line=dict(color='green')))
    fig.update_layout(
        title="연도별 매출, 판관비 및 당기순이익",
        xaxis_title="년도",
        yaxis_title="금액 (억 단위)",
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
        yaxis=dict(tickformat=".1f"),
    )
    html_file = os.path.join(output_dir_html, "연도별_재무상태표.html")
    fig.write_html(html_file)
    png_file = os.path.join(output_dir_png, "연도별_재무상태표.png")
    fig.write_image(png_file, width=1800, height=1170)


# ----------------------------
# Category-wise Analysis
# ----------------------------

def analyze_category(net_profit, sales_data, oracle_item, output_dir, output_dir_html, output_dir_png):
    """
    Perform category-wise sales analysis and generate corresponding plots.
    """
    all_years_category_data = []  # 전체 연도의 카테고리 데이터를 저장할 리스트

    for year in sorted(net_profit['년도'].dropna().unique()):
        # `년도`를 정수형으로 변환
        year = int(year)

        year_data = sales_data[sales_data['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)

        # Group by category for year (억 단위로 변환 전 데이터)
        sales_price_by_category_raw = (
            year_data.groupby('품명')['공급가액']
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        sales_price_by_category_raw = pd.merge(sales_price_by_category_raw, oracle_item, on="품명", how="left")
        sales_price_by_category_raw = (
            sales_price_by_category_raw.groupby("카테고리")["공급가액"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        sales_price_by_category_raw['년도'] = year  # 연도 추가

        # Append to all_years_category_data
        all_years_category_data.append(sales_price_by_category_raw)

        # Save to Excel for category (억 단위 변환 전)
        category_output_path = os.path.join(output_dir, f"{year}_카테고리별_판매량.xlsx")
        sales_price_by_category_raw.to_excel(category_output_path, index=False)
        print(f"{year}년 카테고리별 판매량 Excel 파일 저장 완료: {category_output_path}")

        # Convert to 억 단위 for plotting
        sales_price_by_category = sales_price_by_category_raw.copy()
        sales_price_by_category['공급가액'] /= 1e8

        # Plotly Bar Chart for category
        fig = go.Figure(
            data=[
                go.Bar(
                    x=sales_price_by_category['카테고리'],
                    y=sales_price_by_category['공급가액'],
                    marker=dict(color='skyblue'),
                    text=sales_price_by_category['공급가액'].round(2),
                    textposition='auto',
                )
            ]
        )
        fig.update_layout(
            title=f"{year}년 카테고리별 공급가액 합계",
            xaxis_title="카테고리",
            yaxis_title="공급가액 (억 단위)",
            font=dict(family="Arial, sans-serif", size=12),
            margin=dict(l=50, r=50, t=50, b=100),
            yaxis=dict(tickformat=".1f"),
        )

        # Save category plots
        category_html_file = os.path.join(year_dir_html, f"{year}_카테고리별_판매량.html")
        category_png_file = os.path.join(year_dir_png, f"{year}_카테고리별_판매량.png")
        save_plotly_fig(fig, category_html_file, category_png_file)

    # Save all years category data to a single Excel file
    all_years_category_df = pd.concat(all_years_category_data, ignore_index=True)

    # 전체 연도 카테고리별 총합 계산
    total_category_sum = (
        all_years_category_df.groupby("카테고리")["공급가액"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    # Save total sum to Excel
    total_category_output_path = os.path.join(output_dir, "연도별_카테고리별_판매량.xlsx")
    total_category_sum.to_excel(total_category_output_path, index=False)
    print(f"연도별 연도 카테고리별 판매량 Excel 파일 저장 완료: {total_category_output_path}")

    total_category_sum['공급가액'] /= 1e8  # 억 단위로 변환



    # Plotly Bar Chart for total category sum
    fig = go.Figure(
        data=[
            go.Bar(
                x=total_category_sum['카테고리'],
                y=total_category_sum['공급가액'],
                marker=dict(color='skyblue'),
                text=total_category_sum['공급가액'].round(2),
                textposition='auto',
            )
        ]
    )
    fig.update_layout(
        title="연도별 연도 카테고리별 공급가액 합계",
        xaxis_title="카테고리",
        yaxis_title="공급가액 (억 단위)",
        font=dict(family="Arial, sans-serif", size=12),
        margin=dict(l=50, r=50, t=50, b=100),
        yaxis=dict(tickformat=".1f"),
    )

    # Save total category sum plot
    total_category_html_file = os.path.join(output_dir_html, "연도별_카테고리_판매량.html")
    total_category_png_file = os.path.join(output_dir_png, "연도별_카테고리_판매량.png")
    save_plotly_fig(fig, total_category_html_file, total_category_png_file)



# ----------------------------
# Age-group-wise Analysis
# ----------------------------

def analyze_age_group(net_profit, merged_data, oracle_data, output_dir, output_dir_html, output_dir_png):
    """
    Perform age-group-wise sales analysis and generate corresponding plots.
    """
    sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1]
    merged_age = pd.merge(sales_administrative, oracle_data, on='유저번호')
    merged_age['년도'] = pd.to_numeric(merged_age['년도'], errors='coerce')
    bins = [10, 20, 30, 40, 50]
    labels = ['10대', '20대', '30대', '40대']
    merged_age['나이대'] = pd.cut(merged_age['나이'], bins=bins, labels=labels, right=False)

    year_age_spending = merged_age.groupby(['년도', '나이대'])['공급가액'].sum().reset_index()

    # Save to Excel
    age_output = os.path.join(output_dir, "나이대별_판매량.xlsx")
    year_age_spending.to_excel(age_output, index=False)
    print(f"나이대별 매출 데이터 Excel 파일 저장 완료: {age_output}")


    # Generate Pie Charts per Year
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
        png_file = os.path.join(year_dir_png, f"{year}_나이대별_매출.png")
        save_plotly_fig(fig, html_file, png_file)

    # Generate Line Chart for Age Groups
    age_aggregated = year_age_spending.pivot(index='년도', columns='나이대', values='공급가액').fillna(0)
    age_aggregated /= 1e8  # Convert to 억 단위

    fig = go.Figure()
    colors = {'10대': 'blue', '20대': 'red', '30대': 'green', '40대': 'yellow'}
    for age_group in ['10대', '20대', '30대', '40대']:
        if age_group in age_aggregated.columns:
            fig.add_trace(go.Scatter(
                x=age_aggregated.index,
                y=age_aggregated[age_group],
                mode='lines+markers',
                name=f'{age_group} 매출',
                line=dict(color=colors.get(age_group, 'black'))
            ))

    fig.update_layout(
        title='연도별 나이대별 매출',
        xaxis_title='년도',
        yaxis_title='금액 (억 단위)',
        yaxis=dict(tickformat='.1f'),
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
    )

    # Save Line Chart
    html_file = os.path.join(output_dir_html, "연도별_나이대별_매출.html")
    png_file = os.path.join(output_dir_png, "연도별_나이대별_매출.png")
    save_plotly_fig(fig, html_file, png_file)

# ----------------------------
# Gender-wise Analysis
# ----------------------------

def analyze_gender(net_profit, merged_data, oracle_data, output_dir, output_dir_html, output_dir_png):
    """
    Perform gender-wise sales analysis and generate corresponding plots.
    """
    sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1]
    merged_gender = pd.merge(sales_administrative, oracle_data, on='유저번호')
    merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')
    year_gender_spending = merged_gender.groupby(['년도', '성별'])['공급가액'].sum().reset_index()

    # Save to Excel
    gender_output = os.path.join(output_dir, "성별별_판매량.xlsx")
    year_gender_spending.to_excel(gender_output, index=False)
    print(f"성별 매출 데이터 Excel 파일 저장 완료: {gender_output}")

    # Generate Pie Charts per Year
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
        png_file = os.path.join(year_dir_png, f"{year}_성별_매출.png")
        save_plotly_fig(fig, html_file, png_file)

    # Generate Line Chart for Gender
    gender_aggregated = year_gender_spending.pivot(index='년도', columns='성별', values='공급가액').fillna(0)
    gender_aggregated /= 1e8  # Convert to 억 단위

    fig = go.Figure()
    colors = {'남': 'blue', '여': 'red'}
    for gender in ['남', '여']:
        if gender in gender_aggregated.columns:
            fig.add_trace(go.Scatter(
                x=gender_aggregated.index,
                y=gender_aggregated[gender],
                mode='lines+markers',
                name=f'{gender} 매출',
                line=dict(color=colors.get(gender, 'black'))
            ))

    fig.update_layout(
        title='연도별 성별 매출',
        xaxis_title='년도',
        yaxis_title='금액 (억 단위)',
        yaxis=dict(tickformat='.1f'),
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
    )

    # Save Line Chart
    html_file = os.path.join(output_dir_html, "연도별_성별_매출.html")
    png_file = os.path.join(output_dir_png, "연도별_성별_매출.png")
    save_plotly_fig(fig, html_file, png_file)

    return year_gender_spending, gender_aggregated
# ----------------------------
# VIP Users Analysis
# ----------------------------

def analyze_vip_users(merged_data, oracle_data, output_dir, output_dir_html, output_dir_png):
    """
    Identify VIP users based on cumulative spending and generate corresponding plots.
    """
    merged_gender = pd.merge(merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1], oracle_data, on='유저번호')
    merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')
    years = merged_gender['년도'].dropna().unique()

    for year in sorted(years):
        year_data = merged_gender[merged_gender['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)

        # Calculate cumulative sum
        sales_user_quantity = (
            year_data.groupby('유저번호')['공급가액']
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        sales_user_quantity['누적금액'] = sales_user_quantity['공급가액'].cumsum()
        sales_user_quantity /= 1e8  # Convert to 억 단위

        # Save to Excel
        output_file_path = os.path.join(output_dir, f"{year}_VIP_유저.xlsx")
        sales_user_quantity.to_excel(output_file_path, index=False)
        print(f"{year}년 VIP 유저 Excel 파일 저장 완료: {output_file_path}")

        # Prepare data for plotting
        max_value = sales_user_quantity['누적금액'].max()
        x_vals = np.linspace(0, 1, len(sales_user_quantity))
        percentages = [0.1, 0.2, 0.3]
        cutoff_indices = [int(np.ceil(len(sales_user_quantity) * p)) for p in percentages]

        # Plotly Area Chart
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=sales_user_quantity['누적금액'],
                fill='tozeroy',
                mode='none',
                fillcolor='skyblue',
                name='누적 금액 (억 단위)'
            )
        )

        for cutoff_index, percent in zip(cutoff_indices, percentages):
            if cutoff_index > 0:
                fig.add_trace(
                    go.Scatter(
                        x=[cutoff_index / len(sales_user_quantity)] * 2,
                        y=[0, sales_user_quantity['누적금액'].iloc[cutoff_index - 1]],
                        mode='lines',
                        line=dict(color='red', dash='dash'),
                        name=f'{int(percent * 100)}% 경계'
                    )
                )

        fig.update_layout(
            title=f"{int(year)}년 상위 유저 소비 금액 누적 영역 그래프 (억 단위)",
            xaxis=dict(
                title="유저 비율",
                tickvals=np.linspace(0, 1, 11),
                ticktext=[f"{int(i * 100)}%" for i in np.linspace(0, 1, 11)]
            ),
            yaxis=dict(
                title="누적 금액 (억원)",
                range=[0, max_value],
                tickformat=".1f"
            ),
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=50, r=50, t=50, b=100)
        )

        # Save plots
        html_file_path = os.path.join(year_dir_html, f"{year}_VIP_유저.html")
        png_file = os.path.join(year_dir_png, f"{year}_VIP_유저.png")
        save_plotly_fig(fig, html_file_path, png_file)

    # Overall VIP Users Analysis
    sales_user_quantity_total = (
        merged_gender.groupby('유저번호')['공급가액']
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    sales_user_quantity_total['누적금액'] = sales_user_quantity_total['공급가액'].cumsum()
    sales_user_quantity_total /= 1e8  # Convert to 억 단위

    # Save to Excel
    output_file_path = os.path.join(output_dir, "연도별_VIP_유저.xlsx")
    sales_user_quantity_total.to_excel(output_file_path, index=False)
    print(f"연도별 VIP 유저 Excel 파일 저장 완료: {output_file_path}")

    # Prepare data for plotting
    max_value = sales_user_quantity_total['누적금액'].max()
    x_vals = np.linspace(0, 1, len(sales_user_quantity_total))
    percentages = [0.1, 0.2, 0.3]
    cutoff_indices = [int(np.ceil(len(sales_user_quantity_total) * p)) for p in percentages]

    # Plotly Area Chart
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=x_vals,
            y=sales_user_quantity_total['누적금액'],
            fill='tozeroy',
            mode='none',
            fillcolor='skyblue',
            name='누적 금액 (억 단위)'
        )
    )

    for cutoff_index, percent in zip(cutoff_indices, percentages):
        if cutoff_index > 0:
            fig.add_trace(
                go.Scatter(
                    x=[cutoff_index / len(sales_user_quantity_total)] * 2,
                    y=[0, sales_user_quantity_total['누적금액'].iloc[cutoff_index - 1]],
                    mode='lines',
                    line=dict(color='red', dash='dash'),
                    name=f'{int(percent * 100)}% 경계'
                )
            )

    fig.update_layout(
        title="연도별 상위 유저 소비 금액 누적 영역 그래프 (억 단위)",
        xaxis=dict(
            title="유저 비율",
            tickvals=np.linspace(0, 1, 11),
            ticktext=[f"{int(i * 100)}%" for i in np.linspace(0, 1, 11)]
        ),
        yaxis=dict(
            title="누적 금액 (억원)",
            range=[0, max_value],
            tickformat=".1f"
        ),
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=50, r=50, t=50, b=100)
    )

    # Save plots
    html_file_path = os.path.join(output_dir_html, "연도별_VIP_유저.html")
    png_file = os.path.join(output_dir_png, "연도별_VIP_유저.png")
    save_plotly_fig(fig, html_file_path, png_file)

# ----------------------------
# Area-wise Analysis
# ----------------------------

def analyze_area(merged_data, oracle_data, geo_file_path, region_data, output_dir, output_dir_html, output_dir_png):
    """
    Perform area-wise sales analysis and generate corresponding bubble maps.
    """
    # Map 지역 to 지역코드
    oracle_data['지역코드'] = oracle_data['지역'].map(region_data)
    oracle_data['지역코드'] = oracle_data['지역코드'].astype('Int64')  # Nullable Integer

    sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
    merged_user_data = pd.merge(oracle_data, sales_data, on='유저번호')

    # Load GeoJSON and map coordinates
    geo = load_geojson(geo_file_path)

    region_coordinates = map_region_coordinates(geo)

    # Prepare data
    merged_user_area = merged_user_data[['지역코드', '년도', '공급가액']]
    user_supply_sum = merged_user_area.groupby(['지역코드', '년도'])['공급가액'].sum().reset_index()

    # Generate Bubble Charts per Year
    for year in sorted(user_supply_sum['년도'].unique()):
        year_data = user_supply_sum[user_supply_sum['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)

        map_center = [35.96, 127.1]  # Center of South Korea
        map_year = folium.Map(location=map_center, zoom_start=7, tiles='cartodbpositron')

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

        # Folium maps are HTML-based and do not support direct PNG exports.
        # To capture PNG, consider using tools like Selenium or headless browsers.
        # Here, we'll skip saving PNG for individual years.

    # Generate Combined Bubble Chart
    user_supply_sum_total = merged_user_area.groupby(['지역코드'])['공급가액'].sum().reset_index()

    combined_map = folium.Map(location=[35.96, 127.1], zoom_start=7, tiles='cartodbpositron')
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

    # Note: Saving Folium maps as PNG requires additional steps not covered here.

# ----------------------------
# Main Processing Function
# ----------------------------

def dynamic_analysis(df):
    results = []
    for i in range(len(df)):
        year = int(df.loc[i, "년도"])  # '년도'를 int 타입으로 변환
        sales = df.loc[i, "매출"]
        cost = df.loc[i, "판관비"]
        profit = df.loc[i, "당기순이익"]

        # 매출 분석
        if i > 0:
            prev_sales = df.loc[i - 1, "매출"]
            sales_change = sales - prev_sales
            sales_change_pct = (sales_change / prev_sales) * 100
            sales_text = f"{year}년 매출은 약 {sales:.2f}억 원으로, "
            if sales_change > 0:
                sales_text += f"전년 대비 {sales_change:.2f}억 원(+{sales_change_pct:.1f}%) 증가하며 성장세를 보였습니다."
            elif sales_change < 0:
                sales_text += f"전년 대비 {abs(sales_change):.2f}억 원(-{abs(sales_change_pct):.1f}%) 감소하며 하락세를 보였습니다."
            else:
                sales_text += "전년과 동일한 수준을 유지했습니다."
        else:
            sales_text = f"{year}년 매출은 약 {sales:.2f}억 원으로, 기준점이 되는 데이터입니다."

        # 판관비 분석
        if i > 0:
            prev_cost = df.loc[i - 1, "판관비"]
            cost_change = cost - prev_cost
            cost_change_pct = (cost_change / prev_cost) * 100
            cost_text = f"판관비는 약 {cost:.2f}억 원으로, "
            if cost_change > 0:
                cost_text += f"전년 대비 {cost_change:.2f}억 원(+{cost_change_pct:.1f}%) 증가했습니다."
            elif cost_change < 0:
                cost_text += f"전년 대비 {abs(cost_change):.2f}억 원(-{abs(cost_change_pct):.1f}%) 감소하며 효율성이 개선되었습니다."
            else:
                cost_text += "전년과 동일한 수준을 유지했습니다."
        else:
            cost_text = f"판관비는 약 {cost:.2f}억 원으로, 기준점이 되는 데이터입니다."

        # 당기순이익 분석
        if i > 0:
            prev_profit = df.loc[i - 1, "당기순이익"]
            profit_change = profit - prev_profit
            profit_change_pct = (profit_change / prev_profit) * 100 if prev_profit != 0 else float("inf")
            profit_text = f"당기순이익은 약 {profit:.2f}억 원으로, "
            if profit_change > 0:
                profit_text += f"전년 대비 {profit_change:.2f}억 원(+{profit_change_pct:.1f}%) 증가했습니다."
            elif profit_change < 0:
                profit_text += f"전년 대비 {abs(profit_change):.2f}억 원(-{abs(profit_change_pct):.1f}%) 감소했습니다."
            else:
                profit_text += "전년과 동일한 수준을 유지했습니다."
        else:
            profit_text = f"당기순이익은 약 {profit:.2f}억 원으로, 기준점이 되는 데이터입니다."

        # 분석 결과 합치기
        result = f"### {year}년 분석\n1. {sales_text}\n2. {cost_text}\n3. {profit_text}\n"
        results.append(result)

    return results

    year_data = gender_sales_data[gender_sales_data['년도'] == year]
    total_sales = year_data['공급가액'].sum()

    results = [f"### {year}년 성별 매출 비중"]
    for _, row in year_data.iterrows():
        percentage = (row['공급가액'] / total_sales) * 100
        gender = "남성" if row['성별'] == '남' else "여성"
        results.append(f"- {gender}: 약 {row['공급가액'] / 1e8:.2f}억 원 ({percentage:.1f}%)")

    return "\n".join(results)

def analyze_category_sales(data):

    results = []
    years = sorted(data['년도'].unique())

    for year in years:
        year_data = data[data['년도'] == year]
        total_sales = year_data['공급가액'].sum()

        results.append(f"### {year}년 카테고리별 공급가액 분석")
        if total_sales == 0:
            results.append(f"- 데이터가 없어 분석할 수 없습니다.")
            continue

        for _, row in year_data.iterrows():
            category = row['카테고리']
            sales = row['공급가액']
            percentage = (sales / total_sales) * 100
            results.append(f"- {category}: 약 {sales / 1e8:.2f}억 원 ({percentage:.1f}%)")

        results.append("\n")  # Add a newline for better readability

    return "\n".join(results)

def process_all_analysis():
    """
    Main function to orchestrate all analysis tasks.
    """
    try:
        # Define paths
        input_file = './merged/merged_data.xlsx'
        output_dir = "./analysis"
        output_dir_html = "./analysis_html"
        output_dir_png = "./analysis_png"
        geo_file_path = './유저/SIG.geojson'

        region_file_path = os.path.join("유저", "region_data.json")
        with open(region_file_path, "r", encoding="utf-8") as f:
            region_data = json.load(f)

        # Create necessary directories
        create_directories(output_dir, output_dir_html, output_dir_png)

        # Retrieve data from Oracle
        oracle_data, oracle_item = retrieve_oracle_data()

        # Load merged data
        merged_data = load_merged_data(input_file)

        # Financial Metrics
        sales_by_year, sales_data = calculate_sales(merged_data)
        cost_by_year = calculate_cost(merged_data)
        net_profit, data_net_profit = calculate_net_profit(sales_by_year, cost_by_year)

        # Save financial metrics
        save_financial_metrics(data_net_profit, output_dir)

        # Generate financial data plots
        plot_financial_data(net_profit, output_dir_html, output_dir_png)

        # Dynamic Analysis
        print("[DEBUG] Starting dynamic analysis...")
        dynamic_results = dynamic_analysis(data_net_profit)
        print("[DEBUG] Dynamic analysis completed.")

        # Save dynamic analysis results
        dynamic_output_path = os.path.join(output_dir, "dynamic_analysis.json")
        with open(dynamic_output_path, "w", encoding="utf-8") as f:
            json.dump(dynamic_results, f, ensure_ascii=False, indent=4)
        print(f"Dynamic analysis results saved: {dynamic_output_path}")

        # Category-wise Analysis
        analyze_category(net_profit, sales_data, oracle_item, output_dir, output_dir_html, output_dir_png)

        # VIP Users Analysis
        analyze_vip_users(merged_data, oracle_data, output_dir, output_dir_html, output_dir_png)

        # Area-wise Analysis
        analyze_area(merged_data, oracle_data, geo_file_path, region_data, output_dir, output_dir_html, output_dir_png)

        return True, "모든 분석 작업이 완료되었습니다."

    except Exception as e:
        # 예외 처리
        print(f"Error in process_all_analysis: {str(e)}")
        return False, str(e)
