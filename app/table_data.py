import pandas as pd
import os
import json
import numpy as np


def generate_json_from_excel(year=None):
    try:
        # 파일 경로 설정
        base_path = './analysis/xlsx'
        summary_files = [f'{base_path}/sale.xlsx', f'{base_path}/cost.xlsx', f'{base_path}/net_profit.xlsx']
        category_file = f'{base_path}/{year}_카테고리별_판매량.xlsx' if year and year != "all" else f'{base_path}/연도별_카테고리별_판매량.xlsx'
        gender_file = f'{base_path}/{year}_성별_매출.xlsx' if year and year != "all" else f'{base_path}/성별별_판매량.xlsx'
        age_file = f'{base_path}/{year}_나이대별_판매량.xlsx' if year and year != "all" else f'{base_path}/나이대별_판매량.xlsx'
        vip_file = f'{base_path}/{year}_VIP_유저.xlsx' if year and year != "all" else f'{base_path}/연도별_VIP_유저.xlsx'

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

        # 성별 데이터 처리
        if os.path.exists(gender_file):
            gender_df = pd.read_excel(gender_file)

            if year == "all":
                # "전체" 행 추가
                total_row = {
                    '성별': '전체',
                    '공급가액': gender_df['공급가액'].sum()
                }
                gender_df = pd.concat([gender_df, pd.DataFrame([total_row])], ignore_index=True)

            gender_json = gender_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{gender_file} not found.")

        # 나이대 데이터 처리
        if os.path.exists(age_file):
            age_df = pd.read_excel(age_file)

            if year == "all":
                # "전체" 행 추가
                total_row = {
                    '나이대': '전체',
                    '공급가액': age_df['공급가액'].sum()
                }
                age_df = pd.concat([age_df, pd.DataFrame([total_row])], ignore_index=True)

            age_json = age_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{age_file} not found.")

        # VIP 유저 데이터 처리
        if os.path.exists(vip_file):
            vip_df = pd.read_excel(vip_file)

            # VIP 유저 분석
            vip_df = vip_df.sort_values(by='공급가액', ascending=False).reset_index(drop=True)
            vip_df['누적금액'] = vip_df['공급가액'].cumsum()

            # JSON 변환: 필요한 열만 포함
            vip_users_json = vip_df[['연도', '비율', '공급가액', '누적금액']].to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{vip_file} not found.")

        # 최종 결과 JSON 생성
        final_json = {
            "summary": summary_json,
            "category_sales": category_json,
            "gender_sales": gender_json,
            "age_group_sales": age_json,
            "vip_users": vip_users_json  # VIP 유저 데이터를 JSON에 포함
        }

        # JSON 저장
        output_dir = './analysis/json'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{year if year else '전체'}_분석결과.json")

        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(final_json, json_file, ensure_ascii=False, indent=4)

        print(f"JSON 파일 저장 완료: {output_file}")

        # 결과 반환
        return final_json

    except Exception as e:
        print(f"[ERROR] Exception in generate_json_from_excel: {str(e)}")
        raise
