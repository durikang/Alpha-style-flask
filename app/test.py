import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression

# ----------------------------
# Utility Functions
# ----------------------------
def create_output_paths(base_path="./analysis"):
    paths = {
        "output_dir": base_path,
        "output_dir_xlsx": os.path.join(base_path, "xlsx"),
        "output_dir_html": os.path.join(base_path, "html"),
        "output_dir_png": os.path.join(base_path, "png"),
    }
    for path in paths.values():
        os.makedirs(path, exist_ok=True)
    return paths

def save_excel(dataframe, path):
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False)
        for column in dataframe.columns:
            col_width = max(dataframe[column].astype(str).map(len).max(), len(column)) + 2
            col_idx = dataframe.columns.get_loc(column)
            writer.sheets['Sheet1'].column_dimensions[chr(65 + col_idx)].width = col_width
    print(f"엑셀 파일이 저장되었습니다: {path}")

def save_plotly_fig(fig, html_path, png_path, width=1800, height=1170):
    fig.write_html(html_path)
    print(f"HTML 파일이 저장되었습니다: {html_path}")
    fig.write_image(png_path, width=width, height=height)
    print(f"PNG 파일이 저장되었습니다: {png_path}")

# ----------------------------
# Financial Metrics Calculation
# ----------------------------
def calculate_sales(merged_data):
    sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
    sales_data['년도'] = sales_data['년도'].astype(int)
    sales_data['매출'] = sales_data['수량'] * sales_data['단가']
    sales_by_year = sales_data.groupby('년도')['매출'].sum().reset_index()
    return sales_by_year

def calculate_cost(merged_data):
    cost_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 2].copy()
    cost_data['년도'] = cost_data['년도'].astype(int)
    cost_by_year = cost_data.groupby('년도')['공급가액'].sum().reset_index()
    cost_by_year.rename(columns={'공급가액': '판관비'}, inplace=True)
    return cost_by_year

def calculate_net_profit(sales_by_year, cost_by_year):
    net_profit = pd.merge(sales_by_year, cost_by_year, how='left', on='년도')
    net_profit['당기순이익'] = net_profit['매출'] - net_profit['판관비']
    return net_profit

# ----------------------------
# Predict Future Financial Data
# ----------------------------
def predict_future_data(net_profit, years_to_predict=1):
    model = LinearRegression()

    predictions = {}
    future_years = list(range(int(net_profit['년도'].max()) + 1, int(net_profit['년도'].max()) + 1 + years_to_predict))
    print("Predicting for years:", future_years)  # 디버깅 출력
    for col in ['매출', '판관비', '당기순이익']:
        x = net_profit[['년도']].values
        y = net_profit[col].values
        model.fit(x, y)
        predictions[col] = model.predict(np.array(future_years).reshape(-1, 1))

    future_data = pd.DataFrame({
        '년도': future_years,
        '매출': predictions['매출'],
        '판관비': predictions['판관비'],
        '당기순이익': predictions['당기순이익']
    })
    print("Future Data Generated:")
    print(future_data)  # 디버깅 출력
    return future_data

# ----------------------------
# Plotting Functions
# ----------------------------
def plot_year_with_prediction(historical_data, future_data, output_dir_html, output_dir_png):
    current_year = historical_data['년도'].max()

    fig = go.Figure()

    # Add historical data as the base
    fig.add_trace(go.Bar(
        x=['매출'],
        y=[historical_data['매출'].values[0] / 1e8],
        name=f'{current_year}년(실제)',
        marker=dict(color='red')
    ))
    fig.add_trace(go.Bar(
        x=['판관비'],
        y=[historical_data['판관비'].values[0] / 1e8],
        name=f'{current_year}년(실제)',
        marker=dict(color='blue')
    ))
    fig.add_trace(go.Bar(
        x=['당기순이익'],
        y=[historical_data['당기순이익'].values[0] / 1e8],
        name=f'{current_year}년(실제)',
        marker=dict(color='green')
    ))

    # Add predicted data stacked on top
    fig.add_trace(go.Bar(
        x=['매출'],
        y=[(future_data['매출'] - historical_data['매출'].values[0]) / 1e8],
        name=f'{current_year + 1}년(예측)',
        marker=dict(color='lightpink')
    ))
    fig.add_trace(go.Bar(
        x=['판관비'],
        y=[(future_data['판관비'] - historical_data['판관비'].values[0]) / 1e8],
        name=f'{current_year + 1}년(예측)',
        marker=dict(color='lightblue')
    ))
    fig.add_trace(go.Bar(
        x=['당기순이익'],
        y=[(future_data['당기순이익'] - historical_data['당기순이익'].values[0]) / 1e8],
        name=f'{current_year + 1}년(예측)',
        marker=dict(color='lightgreen')
    ))

    fig.update_layout(
        title=f"{current_year}년 데이터 및 {current_year + 1}년 예측 데이터",
        xaxis_title="항목",
        yaxis_title="금액 (억 단위)",
        barmode='stack',
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
    )

    html_file = os.path.join(output_dir_html, f"{current_year}_예측_그래프.html")
    png_file = os.path.join(output_dir_png, f"{current_year}_예측_그래프.png")

    fig.write_html(html_file)
    fig.write_image(png_file, width=1800, height=1170)

    print(f"{current_year}년 그래프 저장 완료: HTML({html_file}), PNG({png_file})")

def plot_full_prediction_with_actuals(net_profit, future_data, output_dir_html, output_dir_png):
    # '년도'가 정수형인지 확인
    net_profit['년도'] = net_profit['년도'].astype(int)
    future_data['년도'] = future_data['년도'].astype(int)

    print("Net Profit Data:")
    print(net_profit)
    print("Future Data:")
    print(future_data)

    # 실제 데이터와 예측 데이터를 분리
    historical_data = net_profit.copy()
    predicted_data = future_data.copy()

    fig = go.Figure()

    # 각 항목별로 실제 데이터와 예측 데이터를 별도로 추가
    for col, color in zip(['매출', '판관비', '당기순이익'], ['red', 'blue', 'green']):
        # 실제 데이터 (실선)
        fig.add_trace(go.Scatter(
            x=historical_data['년도'],
            y=historical_data[col] / 1e8,  # 억 단위로 변환
            mode='lines+markers',
            name=f'{col} (실제)',
            line=dict(color=color, dash='solid')
        ))

        # 예측 데이터 (점선) - 마지막 실제 포인트와 예측 포인트를 포함
        last_actual_year = historical_data['년도'].max()
        last_actual_value = historical_data[historical_data['년도'] == last_actual_year][col].values[0] / 1e8
        predicted_year = predicted_data['년도'].values[0]
        predicted_value = predicted_data[col].values[0] / 1e8

        # 예측 트레이스에 마지막 실제 포인트 추가
        fig.add_trace(go.Scatter(
            x=[last_actual_year, predicted_year],
            y=[last_actual_value, predicted_value],
            mode='lines+markers',
            name=f'{col} (예측)',
            line=dict(color=color, dash='dash'),
            marker=dict(symbol='diamond', size=10)
        ))

    fig.update_layout(
        title="전체 데이터 및 예측 데이터 (2025년 점선 표시)",
        xaxis_title="년도",
        yaxis_title="금액 (억 단위)",
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
        hovermode='x unified'
    )

    html_file = os.path.join(output_dir_html, "전체_예측_그래프.html")
    png_file = os.path.join(output_dir_png, "전체_예측_그래프.png")

    fig.write_html(html_file)
    fig.write_image(png_file, width=1800, height=1170)

    print(f"전체 예측 그래프 저장 완료: HTML({html_file}), PNG({png_file})")



# ----------------------------
# Main Processing Function
# ----------------------------
def process_all_analysis():
    try:
        input_file = './merged/merged_data.xlsx'
        output_paths = create_output_paths()
        output_dir_html = output_paths["output_dir_html"]
        output_dir_png = output_paths["output_dir_png"]
        output_dir_xlsx = output_paths["output_dir_xlsx"]

        merged_data = pd.read_excel(input_file)
        sales_by_year = calculate_sales(merged_data)
        cost_by_year = calculate_cost(merged_data)
        net_profit = calculate_net_profit(sales_by_year, cost_by_year)

        # '년도' 컬럼이 정수형인지 확인
        net_profit['년도'] = net_profit['년도'].astype(int)

        # Add prediction columns to the DataFrame
        net_profit['예측매출'] = 0
        net_profit['예측판관비'] = 0
        net_profit['예측당기순이익'] = 0

        # 루프 내에서 예측 및 플롯 (개별 연도 예측)
        for year in net_profit['년도'].unique():
            historical_data = net_profit[net_profit['년도'] == year]
            future_data = predict_future_data(net_profit[net_profit['년도'] <= year], years_to_predict=1)

            # '년도'가 정수형인지 확인
            future_data['년도'] = future_data['년도'].astype(int)

            net_profit.loc[net_profit['년도'] == year, '예측매출'] = future_data['매출'].values[0]
            net_profit.loc[net_profit['년도'] == year, '예측판관비'] = future_data['판관비'].values[0]
            net_profit.loc[net_profit['년도'] == year, '예측당기순이익'] = future_data['당기순이익'].values[0]

            plot_year_with_prediction(historical_data, future_data.iloc[0], output_dir_html, output_dir_png)

        # 루프 후에 전체 예측 (최종 예측)
        final_future_data = predict_future_data(net_profit, years_to_predict=1)

        # '년도'가 정수형인지 확인
        final_future_data['년도'] = final_future_data['년도'].astype(int)

        plot_full_prediction_with_actuals(net_profit, final_future_data, output_dir_html, output_dir_png)

        # Save updated data with predictions to Excel
        save_excel(net_profit, os.path.join(output_dir_xlsx, "재무데이터_예측포함.xlsx"))

        print("모든 분석 작업이 완료되었습니다.")
        return True, "모든 분석 작업이 완료되었습니다."
    except Exception as e:
        print(f"Error in process_all_analysis: {str(e)}")
        return False, str(e)
