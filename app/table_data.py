import pandas as pd
import os

def generate_json_from_excel(year=None):
    try:
        # 파일 경로 설정
        summary_files = ['./analysis/sale.xlsx', './analysis/cost.xlsx', './analysis/net_profit.xlsx']
        category_base_path = './analysis'
        category_file = f'{category_base_path}/{year}_카테고리별_판매량.xlsx' if year and year != "all" else f'{category_base_path}/연도별_카테고리별_판매량.xlsx'

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

                # "전체" 데이터 추가 (all 요청 시)
                if year == "all":
                    total_row = {
                        '년도': '전체',
                        df.columns[1]: df[df.columns[1]].sum()  # 열의 합계 계산
                    }
                    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

                summary_data.append(df)
            else:
                raise FileNotFoundError(f"{file} not found.")

        # 병합 로직
        merged_df = pd.concat(summary_data, axis=1)

        # 중복된 '년도' 열 제거
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

        # 열 이름 설정
        merged_df.columns = ['년도', '매출', '판관비', '당기순이익']

        # 특정 연도 필터링
        if year and year != "all":
            filtered_df = merged_df[merged_df['년도'] == int(year)]
        else:
            filtered_df = merged_df

        # JSON 변환
        summary_json = filtered_df.to_dict(orient='records')

        # 카테고리 데이터 처리
        if os.path.exists(category_file):
            category_df = pd.read_excel(category_file)

            if year == "all":
                # "전체" 행 추가
                total_row = {
                    '카테고리': '전체',
                    '공급가액': category_df['공급가액'].sum()
                }
                category_df = pd.concat([category_df, pd.DataFrame([total_row])], ignore_index=True)

            category_json = category_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{category_file} not found.")

        return {"summary": summary_json, "category_sales": category_json}

    except Exception as e:
        print(f"[ERROR] Exception in generate_json_from_excel: {str(e)}")
        raise
