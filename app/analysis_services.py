'''import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import json
import folium
import cx_Oracle
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging


# ----------------------------
# Utility Functions
# ----------------------------
def create_output_paths(base_path="./analysis"):
    """
    Create and return paths for output directories (base, html, png).
    """
    paths = {
        "output_dir": base_path,
        "output_dir_xlsx": os.path.join(base_path, "xlsx"),
        "output_dir_html": os.path.join(base_path, "html"),
        "output_dir_png": os.path.join(base_path, "png"),
    }
    for path in paths.values():
        os.makedirs(path, exist_ok=True)
    return paths

def create_directories(output_dir, output_dir_xlsx, output_dir_html, output_dir_png):
    """
    Create necessary output directories if they do not exist.
    """
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_xlsx, exist_ok=True)
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
    연도별 매출 합계 계산.
    """
    sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
    sales_data['년도'] = sales_data['년도'].astype(str).str.extract(r'(\d{4})')[0].astype(float)
    sales_data['공급가액'] = pd.to_numeric(sales_data['공급가액'], errors='coerce')
    sales_data['수량'] = pd.to_numeric(sales_data['수량'], errors='coerce')
    sales_data['단가'] = pd.to_numeric(sales_data['단가'], errors='coerce')
    sales_data['매출'] = sales_data['수량'] * sales_data['단가']
    sales_by_year = sales_data.groupby('년도')['매출'].sum().reset_index()
    sales_by_year.rename(columns={'매출': '매출'}, inplace=True)
    return sales_by_year, sales_data


def calculate_cost(merged_data):
    """
    연도별 비용 합계 계산.
    """
    cost_data = merged_data[
        (merged_data['매입매출구분(1-매출/2-매입)'] == 2) |
        (merged_data['판매비와 관리비'].notna())
    ].copy()
    cost_data['년도'] = cost_data['년도'].astype(str).str.extract(r'(\d{4})')[0].astype(float)
    cost_data['판매비와 관리비'] = pd.to_numeric(cost_data['판매비와 관리비'], errors='coerce')
    cost_data['공급가액'] = pd.to_numeric(cost_data['공급가액'], errors='coerce')
    cost_data['수량'] = pd.to_numeric(cost_data['수량'], errors='coerce')
    cost_data['단가'] = pd.to_numeric(cost_data['단가'], errors='coerce')
    cost_data['매출'] = cost_data['수량'] * cost_data['단가']

    cost_by_year = cost_data.groupby('년도').agg(
        매입_합계=('매출', 'sum'),
        판관비_합계=('판매비와 관리비', 'sum')
    ).reset_index()
    cost_by_year['판관비'] = cost_by_year['매입_합계'] + cost_by_year['판관비_합계']
    cost_by_year = cost_by_year[['년도', '판관비']]
    return cost_by_year


def calculate_net_profit(sales_by_year, cost_by_year):
    """
    매출과 비용 데이터를 병합하여 당기순이익 계산.
    """
    net_profit = pd.merge(sales_by_year, cost_by_year, how='left', on='년도')
    net_profit['당기순이익'] = net_profit['매출'] - net_profit['판관비']
    net_profit.fillna(0, inplace=True)
    data_net_profit = net_profit.copy()  # 원본 데이터를 유지

    print(data_net_profit)  # 디버깅용 출력

    # 억 단위로 변환
    net_profit[['매출', '판관비', '당기순이익']] /= 1e8

    return net_profit, data_net_profit


def save_financial_metrics(data_net_profit, output_dir_xlsx):
    """
    재무 지표(매출, 비용, 당기순이익)를 전체 및 연도별로 Excel 파일로 저장.
    """
    # 전체 데이터 저장
    sale_output = os.path.join(output_dir_xlsx, "sale.xlsx")
    cost_output = os.path.join(output_dir_xlsx, "cost.xlsx")
    net_profit_output = os.path.join(output_dir_xlsx, "net_profit.xlsx")

    # 억 단위 변환 전 데이터 저장
    data_net_profit[['년도', '매출']].to_excel(sale_output, index=False)
    data_net_profit[['년도', '판관비']].to_excel(cost_output, index=False)
    data_net_profit[['년도', '당기순이익']].to_excel(net_profit_output, index=False)

    print(f"전체 재무 지표 Excel 파일 저장 완료: {sale_output}, {cost_output}, {net_profit_output}")

    os.makedirs(output_dir_xlsx, exist_ok=True)

    # 연도별 데이터를 저장
    for year in sorted(data_net_profit['년도'].dropna().unique()):
        # 연도별 디렉토리 생성
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(int(year)))
        os.makedirs(year_dir_xlsx, exist_ok=True)  # Ensure year directory exists

        # 해당 연도의 데이터 필터링
        yearly_data = data_net_profit[data_net_profit['년도'] == year]
        yearly_output = os.path.join(year_dir_xlsx, f"{int(year)}_재무지표.xlsx")

        # Excel 파일 저장
        yearly_data.to_excel(yearly_output, index=False)
        print(f"{int(year)}년 재무 지표 Excel 파일 저장 완료: {yearly_output}")

    # 전체 데이터를 통합 파일로 저장 (옵션)
    total_output = os.path.join(output_dir_xlsx, "연도별_재무지표.xlsx")
    data_net_profit.to_excel(total_output, index=False)
    print(f"전체 재무 지표 Excel 파일 저장 완료: {total_output}")

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



def analyze_category(net_profit, sales_data, oracle_item, output_dir_xlsx, output_dir_html, output_dir_png):
    """
    Perform category-wise sales analysis and generate corresponding plots.
    """
    all_years_category_data = []  # 연도별 연도의 카테고리 데이터를 저장할 리스트

    for year in sorted(net_profit['년도'].dropna().unique()):
        # `년도`를 정수형으로 변환
        year = int(year)

        year_data = sales_data[sales_data['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(year))  # Directory for Excel
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)
        os.makedirs(year_dir_xlsx, exist_ok=True)  # Ensure Excel directory exists

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
        category_output_path = os.path.join(year_dir_xlsx, f"{year}_카테고리별_판매량.xlsx")
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

    # 연도별 연도 카테고리별 총합 계산
    total_category_sum = (
        all_years_category_df.groupby("카테고리")["공급가액"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )


    # Save total sum to Excel
    total_category_output_path = os.path.join(output_dir_xlsx, "연도별_카테고리별_판매량.xlsx")
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
    total_category_html_file = os.path.join(output_dir_html, "연도별_카테고리별_판매량.html")
    total_category_png_file = os.path.join(output_dir_png, "연도별_카테고리별_판매량.png")
    save_plotly_fig(fig, total_category_html_file, total_category_png_file)

# ----------------------------
# Age-group-wise Analysis
# ----------------------------

def analyze_age_group(net_profit, merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png):
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

    # Save 연도별 나이대별 매출 데이터를 Excel로 저장
    age_output = os.path.join(output_dir_xlsx, "나이대별_판매량.xlsx")
    year_age_spending.to_excel(age_output, index=False)
    print(f"나이대별 매출 데이터 Excel 파일 저장 완료: {age_output}")

    # 연도별 나이대별 데이터를 Excel로 저장
    for year in sorted(year_age_spending['년도'].dropna().unique()):
        year_data = year_age_spending[year_age_spending['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(year))  # Directory for Excel
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)
        os.makedirs(year_dir_xlsx, exist_ok=True)  # Ensure Excel directory exists

        year_excel_output = os.path.join(year_dir_xlsx, f"{year}_나이대별_판매량.xlsx")
        year_data.to_excel(year_excel_output, index=False)
        print(f"{year}년 나이대별 매출 데이터 Excel 파일 저장 완료: {year_excel_output}")



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

def analyze_gender(net_profit, merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png):
    """
    Perform gender-wise sales analysis and generate corresponding plots.
    """
    sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1]
    merged_gender = pd.merge(sales_administrative, oracle_data, on='유저번호')
    merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')
    year_gender_spending = merged_gender.groupby(['년도', '성별'])['공급가액'].sum().reset_index()

    # Save 연도별 성별 매출 데이터를 Excel로 저장
    gender_output = os.path.join(output_dir_xlsx, "성별별_판매량.xlsx")
    year_gender_spending.to_excel(gender_output, index=False)
    print(f"성별 매출 데이터 Excel 파일 저장 완료: {gender_output}")

    # Generate Pie Charts 및 연도별 Excel 파일 생성
    for year in sorted(year_gender_spending['년도'].dropna().unique()):
        year_data = year_gender_spending[year_gender_spending['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(int(year)))
        year_dir_png = os.path.join(output_dir_png, str(int(year)))
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(int(year)))
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)
        os.makedirs(year_dir_xlsx, exist_ok=True)
        print(year_dir_html, year_dir_png, year_dir_xlsx)

        # 연도별 성별 매출 데이터를 Excel로 저장
        year_excel_output = os.path.join(year_dir_xlsx, f"{year}_성별_매출.xlsx")
        year_data.to_excel(year_excel_output, index=False)
        print(f"{year}년 성별 매출 데이터 Excel 파일 저장 완료: {year_excel_output}")

        # 파이 차트 생성
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

        # HTML 및 PNG 파일로 저장
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

# ----------------------------
# VIP Users Analysis
# ----------------------------

def analyze_vip_users(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png):
    """
    Identify VIP users based on cumulative spending and generate corresponding plots.

    Parameters:
    - merged_data: DataFrame containing merged sales and user data
    - oracle_data: DataFrame containing user information
    - output_dir_xlsx: Directory path to save Excel files
    - output_dir_html: Directory path to save HTML plots
    - output_dir_png: Directory path to save PNG plots
    """
    # Merge sales data with user information, filtering for sales transactions
    merged_gender = pd.merge(
        merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1],
        oracle_data,
        on='유저번호',
        how='inner'
    )

    # Ensure '년도' is numeric, coercing errors to NaN
    merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')

    # Extract unique years, excluding NaN
    years = merged_gender['년도'].dropna().unique()

    # Define VIP percentages
    percentages = [0.1, 0.2, 0.3]

    # Iterate over each year to perform analysis
    for year in sorted(years):
        year = int(year)  # Convert to integer for directory naming
        year_data = merged_gender[merged_gender['년도'] == year]

        # Define output directories for the current year
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(year))

        # Create directories if they don't exist
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)
        os.makedirs(year_dir_xlsx, exist_ok=True)

        # Calculate total spending per user and sort descending
        sales_user_quantity = (
            year_data.groupby('유저번호')['공급가액']
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        # Calculate cumulative sum of spending
        sales_user_quantity['누적금액'] = sales_user_quantity['공급가액'].cumsum()

        # Initialize list to store percentage data
        percent_data = []
        total_spending = sales_user_quantity['공급가액'].sum()

        # Calculate top percentages
        for percent in percentages:
            cutoff_index = int(np.ceil(len(sales_user_quantity) * percent))
            if cutoff_index > 0:
                top_users = sales_user_quantity.iloc[:cutoff_index].copy()
                spending = top_users['공급가액'].sum()
                percent_data.append({
                    '연도': year,
                    '비율': f"상위 {int(percent * 100)}%",
                    '공급가액': spending  # '공급가액'으로 컬럼명 변경
                })

        # Save percentage data to Excel
        percent_df = pd.DataFrame(percent_data)
        percent_output_path = os.path.join(year_dir_xlsx, f"{year}_VIP_유저.xlsx")
        percent_df.to_excel(percent_output_path, index=False)
        print(f"{year}년 VIP 유저 데이터 Excel 파일 저장 완료: {percent_output_path}")

        # Prepare data for plotting
        sales_user_quantity['누적금액'] /= 1e8  # Convert to 억 단위 for plotting
        max_value = sales_user_quantity['누적금액'].max()
        x_vals = np.linspace(0, 1, len(sales_user_quantity))
        cutoff_indices = [int(np.ceil(len(sales_user_quantity) * p)) for p in percentages]

        # Create Plotly Figure
        fig = go.Figure()

        # Add cumulative spending area
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

        # Add vertical lines for percentage cutoffs
        for cutoff_index, percent in zip(cutoff_indices, percentages):
            if cutoff_index > 0 and cutoff_index <= len(sales_user_quantity):
                cutoff_x = cutoff_index / len(sales_user_quantity)
                cutoff_y = sales_user_quantity['누적금액'].iloc[cutoff_index - 1]
                fig.add_trace(
                    go.Scatter(
                        x=[cutoff_x, cutoff_x],
                        y=[0, cutoff_y],
                        mode='lines',
                        line=dict(color='red', dash='dash'),
                        name=f'{int(percent * 100)}% 경계'
                    )
                )

        # Update layout for aesthetics
        fig.update_layout(
            title=f"{year}년 상위 유저 소비 금액 누적 영역 그래프 (억 단위)",
            xaxis=dict(
                title="유저 비율",
                tickvals=np.linspace(0, 1, 11),
                ticktext=[f"{int(i * 100)}%" for i in np.linspace(0, 1, 11)],
                range=[0,1]
            ),
            yaxis=dict(
                title="누적 금액 (억원)",
                range=[0, max_value * 1.05],  # Add some padding
                tickformat=".1f"
            ),
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=50, r=50, t=50, b=100)
        )

        # Define file paths for saving plots
        html_file_path = os.path.join(year_dir_html, f"{year}_VIP_유저.html")
        png_file_path = os.path.join(year_dir_png, f"{year}_VIP_유저.png")

        # Save plots using the helper function
        save_plotly_fig(fig, html_file_path, png_file_path)

    # ---- Overall VIP Users Analysis ----

    # Calculate total spending per user across all years
    sales_user_quantity_total = (
        merged_gender.groupby('유저번호')['공급가액']
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    sales_user_quantity_total['누적금액'] = sales_user_quantity_total['공급가액'].cumsum()

    # Initialize list to store overall percentage data
    percent_data_total = []
    total_spending_total = sales_user_quantity_total['공급가액'].sum()

    # Calculate overall top percentages
    for percent in percentages:
        cutoff_index = int(np.ceil(len(sales_user_quantity_total) * percent))
        if cutoff_index > 0:
            top_users_total = sales_user_quantity_total.iloc[:cutoff_index].copy()
            spending_total = top_users_total['공급가액'].sum()
            percent_data_total.append({
                '연도': "연도별",
                '비율': f"상위 {int(percent * 100)}%",
                '공급가액': spending_total  # '공급가액'으로 컬럼명 변경
            })

    # Save overall percentage data to Excel
    percent_df_total = pd.DataFrame(percent_data_total)
    overall_percent_output_path = os.path.join(output_dir_xlsx, "연도별_VIP_유저.xlsx")
    percent_df_total.to_excel(overall_percent_output_path, index=False)
    print(f"연도별 VIP 유저 데이터 Excel 파일 저장 완료: {overall_percent_output_path}")

    # ---- Generate Overall VIP Users Plot ----

    # Prepare data for overall plotting
    sales_user_quantity_total['누적금액'] /= 1e8  # Convert to 억 단위 for plotting
    max_value_total = sales_user_quantity_total['누적금액'].max()
    x_vals_total = np.linspace(0, 1, len(sales_user_quantity_total))
    cutoff_indices_total = [int(np.ceil(len(sales_user_quantity_total) * p)) for p in percentages]

    # Create Plotly Figure for overall analysis
    fig_total = go.Figure()

    # Add cumulative spending area
    fig_total.add_trace(
        go.Scatter(
            x=x_vals_total,
            y=sales_user_quantity_total['누적금액'],
            fill='tozeroy',
            mode='none',
            fillcolor='skyblue',
            name='누적 금액 (억 단위)'
        )
    )

    # Add vertical lines for percentage cutoffs
    for cutoff_index, percent in zip(cutoff_indices_total, percentages):
        if cutoff_index > 0 and cutoff_index <= len(sales_user_quantity_total):
            cutoff_x = cutoff_index / len(sales_user_quantity_total)
            cutoff_y = sales_user_quantity_total['누적금액'].iloc[cutoff_index - 1]
            fig_total.add_trace(
                go.Scatter(
                    x=[cutoff_x, cutoff_x],
                    y=[0, cutoff_y],
                    mode='lines',
                    line=dict(color='darkblue', dash='dash'),
                    name=f'{int(percent * 100)}% 경계'
                )
            )

    # Update layout for aesthetics
    fig_total.update_layout(
        title="전체 연도별 상위 유저 소비 금액 누적 영역 그래프 (억 단위)",
        xaxis=dict(
            title="유저 비율",
            tickvals=np.linspace(0, 1, 11),
            ticktext=[f"{int(i * 100)}%" for i in np.linspace(0, 1, 11)],
            range=[0,1]
        ),
        yaxis=dict(
            title="누적 금액 (억원)",
            range=[0, max_value_total * 1.05],  # Add some padding
            tickformat=".1f"
        ),
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=50, r=50, t=50, b=100)
    )

    # Define file paths for overall plots
    overall_html_path = os.path.join(output_dir_html, "연도별_VIP_유저.html")
    overall_png_path = os.path.join(output_dir_png, "연도별_VIP_유저.png")

    # Save overall plots using the helper function
    save_plotly_fig(fig_total, overall_html_path, overall_png_path)

# ----------------------------
# Area-wise Analysis
# ----------------------------


def save_map_as_png(html_file_path, png_file_path):
    """
    Save a Folium map (HTML) as a PNG file using Selenium.
    """
    # Setup Chrome WebDriver with headless option
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1200x900")

    # Initialize WebDriver
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

    try:
        driver.get(f"file://{os.path.abspath(html_file_path)}")
        time.sleep(2)  # Wait for the map to fully render

        # Take a screenshot of the map
        driver.save_screenshot(png_file_path)
        print(f"PNG saved at '{png_file_path}'")
    finally:
        driver.quit()


def analyze_area(merged_data, oracle_data, geo_file_path, region_data, output_dir_xlsx, output_dir_html,
                 output_dir_png):
    """
    Perform area-wise sales analysis, generate corresponding bubble maps, and save top 5 data to Excel files.
    """
    # Map '지역' to '지역코드'
    oracle_data['지역코드'] = oracle_data['지역'].map(region_data)
    oracle_data['지역코드'] = oracle_data['지역코드'].astype('Int64')  # Nullable Integer

    # Filter sales data where '매입매출구분(1-매출/2-매입)' == 1
    sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
    merged_user_data = pd.merge(oracle_data, sales_data, on='유저번호')

    # Load GeoJSON and map coordinates
    geo = load_geojson(geo_file_path)
    region_coordinates = map_region_coordinates(geo)

    # Prepare aggregated data
    merged_user_area = merged_user_data[['지역코드', '년도', '공급가액', '유저번호']]
    user_supply_sum = merged_user_area.groupby(['지역코드', '년도'])['공급가액'].sum().reset_index()

    # Initialize a dictionary to collect top 5 per year for combined Excel
    combined_top5_dict = {}

    # Generate Bubble Charts and Excel files per Year
    for year in sorted(user_supply_sum['년도'].unique()):
        year_data = user_supply_sum[user_supply_sum['년도'] == year]

        # Create directories for HTML and PNG outputs
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(year))  # Directory for Excel
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)
        os.makedirs(year_dir_xlsx, exist_ok=True)  # Ensure Excel directory exists

        # Initialize Folium map
        map_center = [35.96, 127.1]  # Center of South Korea
        map_year = folium.Map(location=map_center, zoom_start=7, tiles='cartodbpositron')

        for _, row in year_data.iterrows():
            region_code = str(row['지역코드'])
            supply_value = row['공급가액']
            if region_code in region_coordinates:
                lat, lon = region_coordinates[region_code]
                bubble_size = supply_value / 1e6
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=bubble_size,
                    fill=True,
                    fill_color='skyblue',
                    fill_opacity=0.6,
                    stroke=False,
                    popup=f'지역 코드: {region_code}<br>공급가액: {supply_value:,.0f}원'
                ).add_to(map_year)

        # Save map as HTML
        html_file_path = os.path.join(year_dir_html, f'{year}_지역별_판매량.html')
        map_year.save(html_file_path)
        print(f"'{html_file_path}'에 저장 완료")

        # Save map as PNG
        png_file_path = os.path.join(year_dir_png, f'{year}_지역별_판매량.png')
        save_map_as_png(html_file_path, png_file_path)

        # ---- Add Excel Saving Functionality ----

        # Get top 5 regions by '공급가액' for the year
        top5_year = year_data.sort_values(by='공급가액', ascending=False).head(5)
        print(top5_year.columns)

        # Merge with original data to include detailed records
        detailed_top5 = pd.merge(
            merged_user_area,
            top5_year,
            on=['지역코드', '년도'],
            suffixes=('', '_total')
        )

        # Merge with 'oracle_data' to include '지역' names
        detailed_top5 = pd.merge(
            detailed_top5,
            oracle_data[['지역코드', '지역']].drop_duplicates(),
            on='지역코드',
            how='left'
        )

        # Extract the relevant columns for the top 5 areas
        top5_year_area = detailed_top5[['지역', '공급가액']].drop_duplicates()
        # Corrected sort_values with 'ascending' and boolean False
        sum_by_area = top5_year_area.groupby('지역').sum().reset_index().sort_values(by='공급가액', ascending=False)

        top5_year_area = sum_by_area.head(5)
        # Define Excel file path
        excel_file_path = os.path.join(year_dir_xlsx, f'{year}_지역별_판매량.xlsx')

        # Save to Excel
        with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
            top5_year_area.to_excel(writer, sheet_name='상위5_집계', index=False)

        print(f"'{excel_file_path}'에 상위 5개 지역 데이터 저장 완료")

        # Add to combined_top5_dict
        combined_top5_dict[year] = top5_year_area.copy()
        combined_top5_dict[year]['년도'] = year  # Add year information

    # ---- Generate Combined Bubble Chart and Excel ----

    # Aggregate total supply by region
    user_supply_sum_total = merged_user_area.groupby(['지역코드'])['공급가액'].sum().reset_index()

    # Initialize Folium map for combined data
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

    # Save combined map as HTML
    combined_html_path = os.path.join(output_dir_html, "연도별_지역별_판매량.html")
    combined_map.save(combined_html_path)
    print(f"'{combined_html_path}'에 저장 완료")

    # Save combined map as PNG
    combined_png_path = os.path.join(output_dir_png, "연도별_지역별_판매량.png")
    save_map_as_png(combined_html_path, combined_png_path)

    # ---- Add Excel Saving for Combined Data ----

    # Combine all top5 data into a single DataFrame
    combined_top5_df = pd.concat(combined_top5_dict.values(), ignore_index=True)

    combined_top5_df = combined_top5_df.groupby('지역').sum().reset_index()
    # Corrected sort_values with 'ascending' and boolean False
    combined_top5_df = combined_top5_df[['지역', '공급가액']].sort_values(by='공급가액', ascending=False)

    combined_top5_df = combined_top5_df.head(5)
    # Option 1: Save all top5 per year in a single sheet with a '년도' column
    combined_excel_path = os.path.join(output_dir_xlsx, "연도별_지역별_판매량.xlsx")
    with pd.ExcelWriter(combined_excel_path, engine='xlsxwriter') as writer:
        combined_top5_df.to_excel(writer, sheet_name='상위5_집계', index=False)
    print(f"'{combined_excel_path}'에 모든 연도 상위 5개 지역 데이터 저장 완료")
# ----------------------------
# Main Processing Function
# ----------------------------
def process_all_analysis():
    """
    Main function to orchestrate all analysis tasks.
    """
    try:
        # Define paths
        input_file = './merged/merged_data.xlsx'
        geo_file_path = './유저/SIG.geojson'
        region_file_path = './유저/region_data.json'
        output_paths = create_output_paths()
        output_dir = output_paths["output_dir"]
        output_dir_xlsx = output_paths["output_dir_xlsx"]
        output_dir_html = output_paths["output_dir_html"]
        output_dir_png = output_paths["output_dir_png"]

        # Validate paths
        for path in [input_file, geo_file_path, region_file_path]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"Required file not found: {path}")

        # Load region data
        with open(region_file_path, "r", encoding="utf-8") as f:
            region_data = json.load(f)

        # Retrieve data from Oracle
        oracle_data, oracle_item = retrieve_oracle_data()

        # Load merged data
        merged_data = load_merged_data(input_file)

        # Financial Metrics
        sales_by_year, sales_data = calculate_sales(merged_data)
        cost_by_year = calculate_cost(merged_data)
        net_profit, data_net_profit = calculate_net_profit(sales_by_year, cost_by_year)

        # Save financial metrics
        save_financial_metrics(data_net_profit, output_dir_xlsx)

        # Plot financial data
        plot_financial_data(net_profit, output_dir_html, output_dir_png)

        # Category-wise Analysis
        analyze_category(net_profit, sales_data, oracle_item, output_dir_xlsx, output_dir_html, output_dir_png)

        # Age-group-wise Analysis
        analyze_age_group(net_profit, merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png)

        # Gender-wise Analysis
        analyze_gender(net_profit, merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png)

        # VIP Users Analysis
        analyze_vip_users(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png)

        # Area-wise Analysis
        analyze_area(merged_data, oracle_data, geo_file_path, region_data, output_dir_xlsx, output_dir_html, output_dir_png)

        logging.info("모든 분석 작업이 완료되었습니다.")
        return True, "모든 분석 작업이 완료되었습니다."
    except Exception as e:
        logging.error(f"Error in process_all_analysis: {str(e)}")
        return False, str(e)
'''