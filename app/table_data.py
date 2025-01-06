import pandas as pd
import os
import json

def generate_json_from_excel(year=None):
    try:
        # 파일 경로 설정
        summary_files = ['./analysis/sale.xlsx', './analysis/cost.xlsx', './analysis/net_profit.xlsx']
        category_file = './analysis/카테고리별_판매량.xlsx'

        # 데이터를 읽어와 병합
        summary_data = []
        for idx, file in enumerate(summary_files):
            if os.path.exists(file):
                df = pd.read_excel(file)
                if idx == 0:
                    df.columns = ['년도', '매출']  # 첫 번째 파일
                elif idx == 1:
                    df.columns = ['년도', '판관비']  # 두 번째 파일
                elif idx == 2:
                    df.columns = ['년도', '당기순이익']  # 세 번째 파일
                summary_data.append(df)
            else:
                raise FileNotFoundError(f"{file} not found.")

        # 병합 로직
        merged_df = pd.concat(summary_data, axis=1)

        # 중복된 '년도' 열 제거
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

        # 열 개수와 열 이름 확인
        print(f"[DEBUG] Columns in merged_df: {merged_df.columns}")
        print(f"[DEBUG] Number of columns in merged_df: {len(merged_df.columns)}")

        # 열 이름 설정
        merged_df.columns = ['년도', '매출', '판관비', '당기순이익']

        # 특정 연도 필터링
        if year:
            filtered_df = merged_df[merged_df['년도'] == int(year)]
        else:
            filtered_df = merged_df

        # JSON 변환
        summary_json = filtered_df.to_dict(orient='records')

        # 카테고리 데이터 처리
        if os.path.exists(category_file):
            category_df = pd.read_excel(category_file)
            category_json = category_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{category_file} not found.")

        return {"summary": summary_json, "category_sales": category_json}

    except Exception as e:
        print(f"[ERROR] Exception in generate_json_from_excel: {str(e)}")
        raise
