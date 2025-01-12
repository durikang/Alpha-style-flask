import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
import cx_Oracle
from datetime import datetime
from openpyxl.utils import get_column_letter  # 추가된 부분
from flask import Flask, jsonify, request

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
    """
    Save DataFrame to Excel with auto-adjusted column widths and print confirmation.
    """
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False)
        worksheet = writer.sheets['Sheet1']
        for idx, column in enumerate(dataframe.columns, 1):
            max_length = dataframe[column].astype(str).map(len).max()
            adjusted_width = max(max_length, len(column)) + 2
            col_letter = get_column_letter(idx)
            worksheet.column_dimensions[col_letter].width = adjusted_width
    print(f"엑셀 파일이 저장되었습니다: {path}")

def save_plotly_fig(fig, html_path, png_path, width=1800, height=1170):
    """
    Save Plotly figure as HTML and PNG files.
    """
    fig.write_html(html_path)
    print(f"HTML 파일이 저장되었습니다: {html_path}")
    try:
        fig.write_image(png_path, width=width, height=height)
        print(f"PNG 파일이 저장되었습니다: {png_path}")
    except Exception as e:
        print(f"PNG 파일 저장 중 오류 발생: {e}")

# ----------------------------
# Oracle 데이터 조회 함수
# ----------------------------
def retrieve_oracle_data():
    """
    Oracle 데이터베이스에 연결하여 MEMBERS 및 ITEM 데이터를 조회합니다.
    """
    try:
        dsn = cx_Oracle.makedsn("localhost", 1521, service_name="xe")
        connection = cx_Oracle.connect(user="c##finalProject", password="1234", dsn=dsn)
        cursor = connection.cursor()

        # MEMBERS 데이터 조회
        oracle_query_1 = "SELECT BIRTH_DATE, USER_NO, ADDRESS, GENDER FROM MEMBERS"
        cursor.execute(oracle_query_1)
        columns_1 = [col[0] for col in cursor.description]
        data_1 = cursor.fetchall()
        oracle_data = pd.DataFrame(data_1, columns=columns_1)
        oracle_data.replace(['-'], np.nan, inplace=True)
        oracle_data.columns = ["생년월일", "유저번호", "주소", "성별"]

        # ITEM 및 SUB_CATEGORY 데이터 조회
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

        # 나이 계산
        current_year = datetime.now().year
        oracle_data['생년월일'] = pd.to_datetime(oracle_data['생년월일'], errors='coerce')
        oracle_data['나이'] = current_year - oracle_data['생년월일'].dt.year
        oracle_data['나이'] = oracle_data['나이'].fillna(0).astype(int)

        return oracle_data, oracle_item

    except Exception as e:
        print(f"데이터베이스 연결 또는 데이터 조회 중 오류 발생: {e}")
        raise

# ----------------------------
# Financial Metrics Calculation
# ----------------------------
def calculate_sales(merged_data):
    sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()
    sales_data['년도'] = sales_data['년도'].astype(int)
    sales_data['매출'] = sales_data['수량'] * sales_data['단가']
    sales_by_year = sales_data.groupby('년도')['매출'].sum().reset_index()
    return sales_data, sales_by_year

def calculate_cost(merged_data):
    cost_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 2].copy()
    cost_data['년도'] = cost_data['년도'].astype(int)
    cost_by_year = cost_data.groupby('년도')['공급가액'].sum().reset_index()
    cost_by_year.rename(columns={'공급가액': '판관비'}, inplace=True)
    return cost_by_year

def calculate_net_profit(sales_by_year, cost_by_year):
    net_profit = pd.merge(sales_by_year, cost_by_year, how='left', on='년도')
    net_profit['판관비'] = net_profit['판관비'].fillna(0).astype(float)
    net_profit['당기순이익'] = net_profit['매출'] - net_profit['판관비']
    return net_profit

# ----------------------------
# Plotting Functions
# ----------------------------
def plot_year_with_prediction(historical_year, historical_data, future_data, output_dir_html, output_dir_png):
    """
    특정 'year'(historical_year)에 대해,
    - y1: 그 해 실제값
    - y2: (그 해+1) 예측값 - 실제값
    로 스택 그래프.
    """
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=['매출', '판관비', '당기순이익'],
        y=[
            historical_data['매출'] / 1e8,
            historical_data['판관비'] / 1e8,
            historical_data['당기순이익'] / 1e8
        ],
        name=f"{historical_year}년 (실제)",
        marker=dict(color=['red', 'blue', 'green'])
    ))

    fig.add_trace(go.Bar(
        x=['매출', '판관비', '당기순이익'],
        y=[
            (future_data['매출'] - historical_data['매출']) / 1e8,
            (future_data['판관비'] - historical_data['판관비']) / 1e8,
            (future_data['당기순이익'] - historical_data['당기순이익']) / 1e8
        ],
        name=f"{historical_year+1}년 (예측)",
        marker=dict(color=['lightpink', 'lightblue', 'lightgreen'])
    ))

    fig.update_layout(
        title=f"{historical_year}년 vs {historical_year+1}년 (스택예측)",
        xaxis_title="항목",
        yaxis_title="금액 (억 단위)",
        barmode='stack',
        font=dict(size=12),
        legend=dict(orientation="h", y=-0.2),
    )

    html_dir = os.path.join(output_dir_html, str(historical_year))
    png_dir = os.path.join(output_dir_png, str(historical_year))
    os.makedirs(html_dir, exist_ok=True)
    os.makedirs(png_dir, exist_ok=True)

    html_file = os.path.join(html_dir, f"{historical_year}_vs_{historical_year+1}_예측_그래프.html")
    png_file = os.path.join(png_dir, f"{historical_year}_vs_{historical_year+1}_예측_그래프.png")

    save_plotly_fig(fig, html_file, png_file)

def plot_full_prediction_with_actuals(net_profit, output_dir_html, output_dir_png):
    """
    net_profit에 (년도, 매출, 판관비, 당기순이익, 예측매출, 예측판관비, 예측당기순이익) 모두 있으면,
    - 실제 데이터를 라인
    - 예측 데이터(년도별)를 다이아몬드 등으로
    """
    fig = go.Figure()

    # 억단위로 변환 편의
    net_profit = net_profit.copy()
    for col in ['매출','판관비','당기순이익','예측매출','예측판관비','예측당기순이익']:
        net_profit[col] = net_profit[col] / 1e8

    years = net_profit['년도'].unique()
    net_profit.sort_values('년도', inplace=True)

    # 실제
    fig.add_trace(go.Scatter(
        x=net_profit['년도'],
        y=net_profit['매출'],
        mode='lines+markers',
        name='매출(실제)',
        line=dict(color='red')
    ))
    fig.add_trace(go.Scatter(
        x=net_profit['년도'],
        y=net_profit['판관비'],
        mode='lines+markers',
        name='판관비(실제)',
        line=dict(color='blue')
    ))
    fig.add_trace(go.Scatter(
        x=net_profit['년도'],
        y=net_profit['당기순이익'],
        mode='lines+markers',
        name='당기순이익(실제)',
        line=dict(color='green')
    ))

    # 예측
    fig.add_trace(go.Scatter(
        x=net_profit['년도'],
        y=net_profit['예측매출'],
        mode='markers+lines',
        name='매출(예측)',
        line=dict(dash='dot', color='red'),
        marker=dict(symbol='diamond', size=8, color='red')
    ))
    fig.add_trace(go.Scatter(
        x=net_profit['년도'],
        y=net_profit['예측판관비'],
        mode='markers+lines',
        name='판관비(예측)',
        line=dict(dash='dot', color='blue'),
        marker=dict(symbol='diamond', size=8, color='blue')
    ))
    fig.add_trace(go.Scatter(
        x=net_profit['년도'],
        y=net_profit['예측당기순이익'],
        mode='markers+lines',
        name='당기순이익(예측)',
        line=dict(dash='dot', color='green'),
        marker=dict(symbol='diamond', size=8, color='green')
    ))

    fig.update_layout(
        title="전체 재무데이터 (실제+예측)",
        xaxis_title="년도",
        yaxis_title="금액 (억)",
        hovermode='x unified'
    )

    html_file = os.path.join(output_dir_html, "전체_재무데이터_예측.html")
    png_file = os.path.join(output_dir_png, "전체_재무데이터_예측.png")
    save_plotly_fig(fig, html_file, png_file)

# ----------------------------
# Gender Analysis Function
# ----------------------------
def analyze_gender(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png):
    """
    Perform gender-wise sales analysis, perform linear regression prediction,
    and generate corresponding plots and save data.
    """
    # Filter sales data
    sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()

    # Merge with oracle_data to get user information
    merged_gender = pd.merge(sales_administrative, oracle_data, on='유저번호')

    # Ensure '년도' is numeric
    merged_gender['년도'] = pd.to_numeric(merged_gender['년도'], errors='coerce')

    # Aggregate sales by year and gender
    year_gender_spending = merged_gender.groupby(['년도', '성별'])['공급가액'].sum().reset_index()

    # Save aggregated data to Excel (original values, not divided by 1e8)
    gender_output = os.path.join(output_dir_xlsx, "성별별_판매량.xlsx")
    save_excel(year_gender_spending, gender_output)
    print(f"성별 매출 데이터 Excel 파일 저장 완료: {gender_output}")

    # Save individual year and gender data to separate Excel files and generate pie charts
    for year in sorted(year_gender_spending['년도'].dropna().unique()):
        year = int(year)
        year_data = year_gender_spending[year_gender_spending['년도'] == year]
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(year))  # Directory for Excel
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)
        os.makedirs(year_dir_xlsx, exist_ok=True)  # Ensure Excel directory exists
        print(year_dir_html, year_dir_png, year_dir_xlsx)

        # Save to Excel (original values)
        year_excel_output = os.path.join(year_dir_xlsx, f"{year}_성별_매출.xlsx")
        save_excel(year_data, year_excel_output)
        print(f"{year}년 성별 매출 데이터 Excel 파일 저장 완료: {year_excel_output}")

        # Generate and save pie chart (using original values)
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

    # Generate and save line chart for gender (divide by 1e8 only for plotting)
    gender_aggregated = year_gender_spending.pivot(index='년도', columns='성별', values='공급가액').fillna(0)
    gender_aggregated /= 1e8  # Convert to 억 단위

    fig = go.Figure()
    colors = {'남': 'blue', '여': 'red'}
    for gender in ['남', '여']:
        if gender in gender_aggregated.columns:
            fig.add_trace(go.Scatter(
                x=gender_aggregated.index.astype(int),
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

    # Save line chart
    html_file = os.path.join(output_dir_html, "연도별_성별_매출.html")
    png_file = os.path.join(output_dir_png, "연도별_성별_매출.png")
    save_plotly_fig(fig, html_file, png_file)

    # ----------------------------
    # Linear Regression Prediction
    # ----------------------------
    # Prepare data for prediction
    gender_aggregated = gender_aggregated.reset_index()
    gender_aggregated['년도'] = gender_aggregated['년도'].astype(int)

    # Initialize prediction columns
    prediction_columns = {'남': '예측남', '여': '예측여'}
    for gender, pred_col in prediction_columns.items():
        gender_aggregated[pred_col] = 0.0

    # Perform linear regression for each gender
    for gender in ['남', '여']:
        pred_col = prediction_columns[gender]
        # Prepare training data
        df = gender_aggregated[['년도', gender]].dropna()
        if len(df) < 2:
            # Not enough data to predict
            gender_aggregated[pred_col] = gender_aggregated[gender]
            print(f"데이터가 부족하여 {gender}의 예측을 실제 값으로 설정합니다.")
            continue
        X = df[['년도']].values
        y = df[gender].values
        lr = LinearRegression()
        lr.fit(X, y)
        # Predict next year
        future_year = gender_aggregated['년도'] + 1
        predictions = lr.predict(future_year.values.reshape(-1,1))
        # Assign predictions to the DataFrame
        gender_aggregated[pred_col] = predictions
        print(f"{gender}의 매출 예측이 완료되었습니다.")

    # Save predictions to Excel (original values)
    prediction_output = os.path.join(output_dir_xlsx, "성별별_판매량_예측.xlsx")
    save_excel(gender_aggregated, prediction_output)
    print(f"성별별 매출 예측 데이터 Excel 파일 저장 완료: {prediction_output}")

    # Generate and save plots for each gender (divide by 1e8 only for plotting)
    for gender in ['남', '여']:
        pred_col = prediction_columns[gender]
        if gender not in gender_aggregated.columns:
            continue
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=gender_aggregated['년도'],
            y=gender_aggregated[gender],
            mode='lines+markers',
            name=f'{gender} 매출 (실제)',
            line=dict(color='blue')
        ))
        fig.add_trace(go.Scatter(
            x=gender_aggregated['년도'],
            y=gender_aggregated[pred_col],
            mode='lines+markers',
            name=f'{gender} 매출 (예측)',
            line=dict(dash='dot', color='orange')
        ))
        fig.update_layout(
            title=f"{gender} 연도별 매출 (실제 vs 예측)",
            xaxis_title='년도',
            yaxis_title='금액 (억 단위)',
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),
        )

        html_file = os.path.join(output_dir_html, f"{gender}_매출_예측.html")
        png_file = os.path.join(output_dir_png, f"{gender}_매출_예측.png")
        save_plotly_fig(fig, html_file, png_file)

    # Generate and save combined line chart for all genders with predictions
    fig = go.Figure()
    colors = {'남': 'blue', '여': 'red'}
    for gender in ['남', '여']:
        pred_col = prediction_columns[gender]
        if gender in gender_aggregated.columns:
            fig.add_trace(go.Scatter(
                x=gender_aggregated['년도'],
                y=gender_aggregated[gender],
                mode='lines+markers',
                name=f'{gender} 매출 (실제)',
                line=dict(color=colors.get(gender, 'black'))
            ))
            fig.add_trace(go.Scatter(
                x=gender_aggregated['년도'],
                y=gender_aggregated[pred_col],
                mode='lines+markers',
                name=f'{gender} 매출 (예측)',
                line=dict(dash='dot', color=colors.get(gender, 'black'))
            ))

    fig.update_layout(
        title='연도별 성별 매출 (실제 + 예측)',
        xaxis_title='년도',
        yaxis_title='금액 (억 단위)',
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
    )

    # Save the combined line chart
    html_file = os.path.join(output_dir_html, "연도별_성별_매출_예측.html")
    png_file = os.path.join(output_dir_png, "연도별_성별_매출_예측.png")
    save_plotly_fig(fig, html_file, png_file)

# ----------------------------
# Age Group Analysis Function
# ----------------------------
def analyze_age_group(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png):
    """
    Perform age-group-wise sales analysis, perform linear regression prediction,
    and generate corresponding plots and save data.
    """
    # Filter sales data
    sales_administrative = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()

    # Merge with oracle_data to get user information
    merged_age = pd.merge(sales_administrative, oracle_data, on='유저번호')

    # Ensure '년도' is numeric
    merged_age['년도'] = pd.to_numeric(merged_age['년도'], errors='coerce')

    # Define age groups
    bins = [10, 20, 30, 40, 50]
    labels = ['10대', '20대', '30대', '40대']
    merged_age['나이대'] = pd.cut(merged_age['나이'], bins=bins, labels=labels, right=False)

    # Aggregate sales by year and age group
    year_age_spending = merged_age.groupby(['년도', '나이대'])['공급가액'].sum().reset_index()

    # Save aggregated data to Excel (original values, not divided by 1e8)
    age_output = os.path.join(output_dir_xlsx, "나이대별_판매량.xlsx")
    save_excel(year_age_spending, age_output)
    print(f"나이대별 매출 데이터 Excel 파일 저장 완료: {age_output}")

    # ... (중간 코드 생략)

    # Generate and save line chart for age groups (divide by 1e8 only for plotting)
    age_aggregated = year_age_spending.pivot(index='년도', columns='나이대', values='공급가액').fillna(0)
    age_aggregated_plot = age_aggregated.copy() / 1e8  # Separate for plotting

    # ... (시각화 코드 생략)

    # ----------------------------
    # Linear Regression Prediction
    # ----------------------------
    # Prepare data for prediction
    age_aggregated_plot = age_aggregated_plot.reset_index()
    age_aggregated_plot['년도'] = age_aggregated_plot['년도'].astype(int)

    # Initialize prediction columns
    prediction_columns = {age_group: f'예측{age_group}' for age_group in ['10대', '20대', '30대', '40대']}
    for age_group, pred_col in prediction_columns.items():
        age_aggregated_plot[pred_col] = 0.0

    # Perform linear regression for each age group
    for age_group in ['10대', '20대', '30대', '40대']:
        pred_col = prediction_columns[age_group]
        # Prepare training data
        df = age_aggregated_plot[['년도', age_group]].dropna()
        if len(df) < 2:
            # Not enough data to predict
            age_aggregated_plot[pred_col] = age_aggregated_plot[age_group]
            print(f"데이터가 부족하여 {age_group}의 예측을 실제 값으로 설정합니다.")
            continue
        X = df[['년도']].values
        y = df[age_group].values
        lr = LinearRegression()
        lr.fit(X, y)
        # Predict next year
        future_year = age_aggregated_plot['년도'] + 1
        predictions = lr.predict(future_year.values.reshape(-1, 1))
        # Assign predictions to the DataFrame
        age_aggregated_plot[pred_col] = predictions
        print(f"Predictions completed for {age_group}.")

    # Save predictions to Excel (원본 데이터, 1e8로 나누지 않음)
    prediction_output = os.path.join(output_dir_xlsx, "나이대별_판매량_예측.xlsx")
    # Merge predictions back to original aggregated data
    age_predictions = pd.merge(age_aggregated, age_aggregated_plot[['년도'] + list(prediction_columns.values())], on='년도', how='left')
    save_excel(age_predictions, prediction_output)
    print(f"나이대별 매출 예측 데이터 Excel 파일 저장 완료: {prediction_output}")


# ----------------------------
# Financial Prediction Function
# ----------------------------
def predict_next_year_for_each_year(net_profit):
    """
    net_profit(년도, 매출, 판관비, 당기순이익) ->
      각 연도별로 (올해 -> 내년) 선형회귀 예측
      예측값을 '예측매출','예측판관비','예측당기순이익' 컬럼에 업데이트
    """
    net_profit = net_profit.copy()
    net_profit['년도'] = net_profit['년도'].astype(int)
    net_profit['예측매출'] = 0.0
    net_profit['예측판관비'] = 0.0
    net_profit['예측당기순이익'] = 0.0

    years = sorted(net_profit['년도'].unique())
    for y in years:
        # 올해보다 작거나 같은 연도로 회귀
        train_df = net_profit[net_profit['년도'] <= y]
        if train_df.empty:
            continue

        # LinearRegression
        lr = LinearRegression()

        future_year = y + 1
        # x,y
        X = train_df[['년도']].values
        # 매출
        lr.fit(X, train_df['매출'].values)
        pred_sale = lr.predict([[future_year]])[0]

        # 판관비
        lr.fit(X, train_df['판관비'].values)
        pred_cost = lr.predict([[future_year]])[0]

        # 당기순이익
        lr.fit(X, train_df['당기순이익'].values)
        pred_profit = lr.predict([[future_year]])[0]

        # net_profit에 올해 행에 '내년 예측치' 입력
        net_profit.loc[ net_profit['년도'] == y, '예측매출'] = pred_sale
        net_profit.loc[ net_profit['년도'] == y, '예측판관비'] = pred_cost
        net_profit.loc[ net_profit['년도'] == y, '예측당기순이익'] = pred_profit

    return net_profit

def analyze_vip_users(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png):
    """
    VIP 유저를 식별하고 누적 소비 금액을 기반으로 선형 회귀를 통해 예측을 수행한 후,
    10% ~ 100% 전 구간의 예측치를 계산하고,
    결과를 엑셀 파일과 그래프로 저장합니다.
    """
    import os
    import pandas as pd
    import numpy as np
    import plotly.graph_objects as go
    from sklearn.linear_model import LinearRegression

    # ----------------------------
    # 그래프 저장을 위한 헬퍼 함수
    # ----------------------------
    def save_plotly_fig(fig, html_path, png_path):
        fig.write_html(html_path)
        fig.write_image(png_path)
        print(f"HTML 파일이 저장되었습니다: {html_path}")
        print(f"PNG 파일이 저장되었습니다: {png_path}")

    # ----------------------------
    # 1) 매출 데이터 필터링
    # ----------------------------
    sales_data = merged_data[merged_data['매입매출구분(1-매출/2-매입)'] == 1].copy()

    # ----------------------------
    # 2) 유저 정보와 병합
    # ----------------------------
    merged_vip = pd.merge(sales_data, oracle_data, on='유저번호', how='inner')

    # ----------------------------
    # 3) '년도' 컬럼을 숫자형으로 변환
    # ----------------------------
    merged_vip['년도'] = pd.to_numeric(merged_vip['년도'], errors='coerce')

    # ----------------------------
    # 4) 예측 비율 설정 (10% ~ 100% 전부)
    # ----------------------------
    # 0.1, 0.2, 0.3, ..., 1.0
    percentages = [i * 0.1 for i in range(1, 11)]

    # ----------------------------
    # 5) 전체 예측 데이터를 저장할 리스트
    # ----------------------------
    percent_data_total = []

    # ----------------------------
    # 6) 연도별 VIP 유저 분석
    #    (연도별로 그래프 및 예측)
    # ----------------------------
    for year in sorted(merged_vip['년도'].dropna().unique()):
        year = int(year)
        year_data = merged_vip[merged_vip['년도'] == year]

        # 디렉토리 설정
        year_dir_html = os.path.join(output_dir_html, str(year))
        year_dir_png = os.path.join(output_dir_png, str(year))
        year_dir_xlsx = os.path.join(output_dir_xlsx, str(year))
        os.makedirs(year_dir_html, exist_ok=True)
        os.makedirs(year_dir_png, exist_ok=True)
        os.makedirs(year_dir_xlsx, exist_ok=True)

        # 유저별 총 소비 금액 계산
        user_spending = (
            year_data
            .groupby('유저번호')['공급가액']
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        # 누적 금액 계산
        user_spending['누적금액'] = user_spending['공급가액'].cumsum()

        # VIP 비율별 예측
        percent_data = []
        for percent in percentages:
            cutoff_index = int(np.ceil(len(user_spending) * percent))
            if cutoff_index > 0:
                top_users = user_spending.iloc[:cutoff_index].copy()
                spending = top_users['공급가액'].sum()

                # 선형 회귀를 위한 데이터
                historical_data = merged_vip[merged_vip['유저번호'].isin(top_users['유저번호'])]
                yearly_spending = historical_data.groupby('년도')['공급가액'].sum().reset_index()

                # 예측 수행
                if len(yearly_spending) >= 2:
                    X = yearly_spending[['년도']].values
                    y = yearly_spending['공급가액'].values
                    lr = LinearRegression()
                    lr.fit(X, y)
                    future_year = yearly_spending['년도'].max() + 1
                    predicted_spending = lr.predict([[future_year]])[0]
                else:
                    # 데이터가 부족하면 마지막 값을 사용
                    predicted_spending = yearly_spending['공급가액'].iloc[-1] if not yearly_spending.empty else 0.0

                percent_data.append({
                    '비율': f"상위 {int(percent*100)}%",
                    '공급가액': spending,
                    '예측': predicted_spending
                })

        # DataFrame으로 변환 & 엑셀 저장
        percent_df = pd.DataFrame(percent_data)
        excel_path = os.path.join(year_dir_xlsx, f"{year}_VIP_유저.xlsx")
        percent_df.to_excel(excel_path, index=False)
        print(f"{year}년 VIP 유저 데이터 Excel 파일 저장 완료: {excel_path}")

        # 누적금액을 억 단위로 변환
        user_spending['누적금액_억'] = user_spending['누적금액'] / 1e8

        # ----------------------------
        # 연도별 VIP 그래프(기존 누적 영역)
        # ----------------------------
        fig = go.Figure()

        # 누적 영역 (하늘색)
        fig.add_trace(
            go.Scatter(
                x=np.linspace(0, 1, len(user_spending)),
                y=user_spending['누적금액_억'],
                fill='tozeroy',
                mode='none',
                fillcolor='skyblue',
                name='누적 금액 (억 단위)'
            )
        )

        # VIP 비율 경계선 (빨간 점선)
        cutoff_indices = [int(np.ceil(len(user_spending) * p)) for p in percentages]
        for cutoff_idx, percent in zip(cutoff_indices, percentages):
            if 0 < cutoff_idx <= len(user_spending):
                cutoff_x = cutoff_idx / len(user_spending)
                cutoff_y = user_spending['누적금액_억'].iloc[cutoff_idx - 1]
                fig.add_trace(
                    go.Scatter(
                        x=[cutoff_x, cutoff_x],
                        y=[0, cutoff_y],
                        mode='lines',
                        line=dict(color='red', dash='dash'),
                        name=f'{int(percent*100)}% 경계'
                    )
                )

        fig.update_layout(
            title=f"{year}년 상위 유저 소비 금액 누적 영역 그래프 (억 단위)",
            xaxis_title="유저 비율",
            yaxis_title="누적 금액 (억원)",
            font=dict(family="Arial, sans-serif", size=12),
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=50, r=50, t=50, b=100)
        )

        # 그래프 저장
        html_file = os.path.join(year_dir_html, f"{year}_VIP_유저.html")
        png_file = os.path.join(year_dir_png, f"{year}_VIP_유저.png")
        save_plotly_fig(fig, html_file, png_file)

        # 전체 예측 데이터에 추가
        for data in percent_data:
            percent_data_total.append({
                '비율': data['비율'],
                '예측': data['예측']
            })

    # ----------------------------
    # 7) 전체 VIP 유저 분석
    # ----------------------------
    sales_user_quantity_total = (
        merged_vip
        .groupby('유저번호')['공급가액']
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    sales_user_quantity_total['누적금액'] = sales_user_quantity_total['공급가액'].cumsum()

    overall_percent_data = []
    for percent in percentages:
        cutoff_index = int(np.ceil(len(sales_user_quantity_total) * percent))
        if cutoff_index > 0:
            top_users_total = sales_user_quantity_total.iloc[:cutoff_index].copy()
            spending_total = top_users_total['공급가액'].sum()

            # 선형 회귀
            historical_data_total = merged_vip[merged_vip['유저번호'].isin(top_users_total['유저번호'])]
            yearly_spending_total = historical_data_total.groupby('년도')['공급가액'].sum().reset_index()

            if len(yearly_spending_total) >= 2:
                X_total = yearly_spending_total[['년도']].values
                y_total = yearly_spending_total['공급가액'].values
                lr_total = LinearRegression()
                lr_total.fit(X_total, y_total)
                future_year_total = yearly_spending_total['년도'].max() + 1
                predicted_spending_total = lr_total.predict([[future_year_total]])[0]
            else:
                predicted_spending_total = yearly_spending_total['공급가액'].iloc[-1] if not yearly_spending_total.empty else 0.0

            overall_percent_data.append({
                '비율': f"상위 {int(percent*100)}%",
                '공급가액': spending_total,
                '예측': predicted_spending_total
            })

    # 전체 DataFrame & 엑셀 저장
    overall_percent_df = pd.DataFrame(overall_percent_data)
    overall_excel_path = os.path.join(output_dir_xlsx, "전체_VIP_유저.xlsx")
    overall_percent_df.to_excel(overall_excel_path, index=False)
    print(f"전체 VIP 유저 데이터 Excel 파일 저장 완료: {overall_excel_path}")

    # ----------------------------
    # 8) 전체 VIP 유저 그래프 생성
    # ----------------------------
    fig_total = go.Figure()

    # (1) 기존 하늘색 누적 영역
    fig_total.add_trace(
        go.Scatter(
            x=np.linspace(0, 1, len(sales_user_quantity_total)),
            y=sales_user_quantity_total['누적금액'] / 1e8,  # 억 단위
            fill='tozeroy',
            mode='none',
            fillcolor='skyblue',
            name='누적 금액 (억 단위)'
        )
    )

    # (2) VIP 비율 경계선(빨간 점선)
    cutoff_indices_total = [int(np.ceil(len(sales_user_quantity_total) * p)) for p in percentages]
    for cutoff_index, percent in zip(cutoff_indices_total, percentages):
        if 0 < cutoff_index <= len(sales_user_quantity_total):
            cutoff_x = cutoff_index / len(sales_user_quantity_total)
            cutoff_y = (sales_user_quantity_total['누적금액'] / 1e8).iloc[cutoff_index - 1]
            fig_total.add_trace(
                go.Scatter(
                    x=[cutoff_x, cutoff_x],
                    y=[0, cutoff_y],
                    mode='lines',
                    line=dict(color='red', dash='dash'),
                    name=f'{int(percent*100)}% 경계'
                )
            )

    # (3) 예측 데이터 영역을 추가 (주황색 영역)
    shapes = []
    bar_width = 0.015  # 막대 폭 (10개가 모두 들어가므로 살짝 줄임)
    for i, (cutoff_index, percent) in enumerate(zip(cutoff_indices_total, percentages)):
        if 0 < cutoff_index <= len(sales_user_quantity_total):
            cutoff_x = cutoff_index / len(sales_user_quantity_total)
            cutoff_y = (sales_user_quantity_total['누적금액'] / 1e8).iloc[cutoff_index - 1]
            predicted_ = overall_percent_data[i]['예측'] / 1e8  # 예측액(억 단위)

            # 직사각형(사각 영역) 만들기
            shape = dict(
                type='rect',
                xref='x',
                yref='y',
                x0=cutoff_x - bar_width,   # 막대 왼쪽
                x1=cutoff_x,              # 막대 오른쪽
                y0=cutoff_y,              # 기존 누적
                y1=cutoff_y + predicted_, # 예측 더해진 높이
                fillcolor='orange',
                opacity=0.6,
                line=dict(width=0)
            )
            shapes.append(shape)

    fig_total.update_layout(
        title="전체 VIP 유저 소비 금액 누적 영역 그래프 (10% ~ 100% 예측)",
        xaxis=dict(title="유저 비율", range=[0,1]),
        yaxis_title="누적 금액 (억원)",
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=50, r=50, t=50, b=100),
        shapes=shapes
    )

    # 그래프 저장
    overall_html_file = os.path.join(output_dir_html, "전체_VIP_유저.html")
    overall_png_file = os.path.join(output_dir_png, "전체_VIP_유저.png")
    save_plotly_fig(fig_total, overall_html_file, overall_png_file)


def process_all_analysis():
    try:
        # 1) 경로 생성
        input_file = './merged/merged_data.xlsx'
        paths = create_output_paths()
        output_dir_xlsx = paths["output_dir_xlsx"]
        output_dir_html = paths["output_dir_html"]
        output_dir_png  = paths["output_dir_png"]

        # 2) DB/엑셀 로딩
        oracle_data, oracle_item = retrieve_oracle_data()
        merged_data = pd.read_excel(input_file)  # 사용자가 가진 merged_data.xlsx

        # 3) 매출,비용,순이익 계산
        sales_data, sales_by_year = calculate_sales(merged_data)
        cost_by_year = calculate_cost(merged_data)
        net_profit = calculate_net_profit(sales_by_year, cost_by_year)

        net_profit['년도'] = net_profit['년도'].astype(int)

        # 4) (올해 -> 내년) 예측
        net_profit = predict_next_year_for_each_year(net_profit)

        # 5) 각 연도별 그래프 (올해 vs 내년)
        min_year = net_profit['년도'].min()
        max_year = net_profit['년도'].max()
        for y in range(min_year, max_year+1):
            # 해당 연도의 실제값
            row_h = net_profit[net_profit['년도'] == y]
            if row_h.empty:
                continue
            # dict화
            hist_data = {
                '매출': row_h['매출'].values[0],
                '판관비': row_h['판관비'].values[0],
                '당기순이익': row_h['당기순이익'].values[0]
            }
            # 올해 행에 "예측"은 (내년 예측)
            pred_data = {
                '매출': row_h['예측매출'].values[0],
                '판관비': row_h['예측판관비'].values[0],
                '당기순이익': row_h['예측당기순이익'].values[0]
            }

            plot_year_with_prediction(y, hist_data, pred_data, output_dir_html, output_dir_png)

            # 6) 전체 그래프
            plot_full_prediction_with_actuals(net_profit, output_dir_html, output_dir_png)

            # 7) 재무데이터 저장
            save_excel(net_profit, os.path.join(output_dir_xlsx, "재무데이터_예측포함.xlsx"))

            # ---------------------------------------------
            # 카테고리별 분석
            # ---------------------------------------------
            # (1) sales_data에 min/max_year 찾기
            min_year_cat = sales_data['년도'].min()
            max_year_cat = sales_data['년도'].max()

            # 카테고리별 (년도, 품목→카테고리, 공급가액) => "올해→내년" 예측
            #  -> ex: 2020행에는 2021 예측, 2021행에는 2022 예측 ...
            #  -> 마지막 연도(예: 2024) 는 2025 예측
            # => 이후 wide 형태로 저장

            # 1) 연도별 카테고리 집계
            oracle_item.columns = ["품명", "SUB_ID", "카테고리"]  # 혹시 다시 맞춤
            merged_cat_df = (
                sales_data
                .groupby(['년도','품명'])['공급가액']
                .sum()
                .reset_index()
                .merge(oracle_item[['품명','카테고리']], on='품명', how='left')
            )
            merged_cat_df['카테고리'] = merged_cat_df['카테고리'].fillna('미분류')

            # 년도+카테고리별 집계
            merged_cat_df = merged_cat_df.groupby(['년도','카테고리'])['공급가액'].sum().reset_index()

            # 전체 연도 범위
            all_years = sorted(merged_cat_df['년도'].unique())  # ex) [2020,2021,2022,2023,2024]
            final_list = []

            # 2) (올해→내년) 예측
            cat_list = merged_cat_df['카테고리'].unique()

            for cat in cat_list:
                cat_data = merged_cat_df[merged_cat_df['카테고리'] == cat].copy()
                cat_data.sort_values('년도', inplace=True)

                # 선형회귀로 cat_data를 학습
                if len(cat_data) < 2:
                    # 데이터가 1개 이하라면 예측 불가능 -> 그냥 실제값 있으면 그대로 두고, 내년도는 0
                    for y in all_years:
                        row_ = cat_data[cat_data['년도'] == y]
                        if len(row_) > 0:
                            real_val = row_['공급가액'].values[0]
                        else:
                            real_val = 0.0
                        # 올해행에는 내년예측이 필요 -> +1년
                        next_pred = real_val  # or 0
                        final_list.append({
                            '카테고리': cat,
                            '년도': y,
                            '실제공급가액': real_val,
                            '예측공급가액': next_pred
                        })
                    continue

                # 데이터 2개 이상 -> 회귀 가능
                X = cat_data[['년도']].values
                y_values = cat_data['공급가액'].values
                lr = LinearRegression()
                lr.fit(X, y_values)

                for y_ in all_years:
                    # 실제
                    row_ = cat_data[cat_data['년도'] == y_]
                    if len(row_) > 0:
                        real_val = row_['공급가액'].values[0]
                    else:
                        real_val = 0.0

                    next_pred = lr.predict([[y_ + 1]])[0]  # y_+1년에 대한 예측

                    final_list.append({
                        '카테고리': cat,
                        '년도': y_,
                        '실제공급가액': real_val,
                        '예측공급가액': next_pred
                    })

            cat_df_final = pd.DataFrame(final_list)
            # -> 여기서 2024년 행에 '예측공급가액'은 2025년 예측
            # ex) 2020행: 2021 예측, 2021행: 2022 예측, ... 2024행: 2025 예측

            # wide pivot
            pivot_actual = cat_df_final.pivot_table(
                index='년도',
                columns='카테고리',
                values='실제공급가액',
                aggfunc='first'
            ).fillna(0)

            pivot_pred = cat_df_final.pivot_table(
                index='년도',
                columns='카테고리',
                values='예측공급가액',
                aggfunc='first'
            ).fillna(0)
            pivot_pred.columns = ['예측' + c for c in pivot_pred.columns]

            # merge
            cat_wide = pd.concat([pivot_actual, pivot_pred], axis=1)
            cat_wide.reset_index(inplace=True)
            # => col: 년도, 단화, 미분류, 상의, 슬리퍼, 운동화, 하의, 예측단화, 예측미분류, 예측상의 ...

            # 정렬
            # 예: 카테고리 순서 임의
            cat_order = ['단화','미분류','상의','슬리퍼','운동화','하의']
            pred_order = ['예측단화','예측미분류','예측상의','예측슬리퍼','예측운동화','예측하의']
            final_cols = ['년도'] + cat_order + pred_order
            # 실제 존재하는지 체크
            final_cols = [c for c in final_cols if c in cat_wide.columns]

            cat_wide = cat_wide[final_cols]

            # 엑셀 저장
            save_excel(cat_wide, os.path.join(output_dir_xlsx, "카테고리별_판매량_예측_가로형.xlsx"))

            # --------- 그래프: 마지막 연도 -> 마지막 연도+1 ---------
            # ex) max_year_cat=2024 -> 2025 예측 그래프
            for y_ in all_years:
                # '년도'=y_ 행에서 내년 예측
                row_ = cat_wide[cat_wide['년도'] == y_]
                if row_.empty:
                    continue

                # 내년(y_+1) 예측 값
                # (개별 카테고리)
                # stacked bar: 올해(실제) vs 내년(예측)
                actual_vals = []
                pred_vals = []

                for cat_ in cat_order:
                    if cat_ in row_.columns:
                        actual_val = row_[cat_].values[0]
                    else:
                        actual_val = 0.0
                    # 예측
                    pred_cat = '예측' + cat_
                    if pred_cat in row_.columns:
                        pred_val = row_[pred_cat].values[0]
                    else:
                        pred_val = 0.0
                    actual_vals.append(actual_val)
                    pred_vals.append(pred_val)

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=cat_order,
                    y=np.array(actual_vals)/1e8,
                    name=f"{y_}년(실제)",
                    marker=dict(color='blue')
                ))
                fig.add_trace(go.Bar(
                    x=cat_order,
                    y=(np.array(pred_vals)-np.array(actual_vals))/1e8,
                    name=f"{y_+1}년(예측)",
                    marker=dict(color='orange')
                ))
                fig.update_layout(
                    title=f"{y_}년 vs {y_+1}년 카테고리별 스택예측",
                    xaxis_title="카테고리",
                    yaxis_title="공급가액(억)",
                    barmode='stack',
                    font=dict(family="Arial, sans-serif", size=12),
                )

                year_dir_html = os.path.join(output_dir_html, str(y_))
                year_dir_png = os.path.join(output_dir_png, str(y_))
                os.makedirs(year_dir_html, exist_ok=True)
                os.makedirs(year_dir_png, exist_ok=True)

                html_file = os.path.join(year_dir_html, f"{y_}_vs_{y_+1}_카테고리별_스택예측.html")
                png_file  = os.path.join(year_dir_png, f"{y_}_vs_{y_+1}_카테고리별_스택예측.png")
                save_plotly_fig(fig, html_file, png_file)

        # 8) 성별별 분석 수행
        analyze_gender(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png)

        # 9) 나이대별 분석 수행
        analyze_age_group(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png)

        # 10) VIP 유저 분석 수행
        analyze_vip_users(merged_data, oracle_data, output_dir_xlsx, output_dir_html, output_dir_png)

        # 모든 분석 작업이 정상적으로 완료된 후 반환
        return True, "모든 분석 작업이 완료되었습니다."


    except FileNotFoundError as e:
        print(e)
        return False, str(e)
    except Exception as e:
        print(f"Error in process_all_analysis: {str(e)}")
        return False, str(e)

