import pandas as pd
import os
import json
import numpy as np

def generate_json_from_excel(year=None):
    try:
        # 파일 경로 설정
        base_path = './analysis/xlsx'
        summary_files = f'{base_path}/{year}/{year}_재무지표.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_재무지표.xlsx'
        # year가 "all"이 아닐 때와 일 때의 파일 경로 설정
        category_file = f'{base_path}/{year}/{year}_카테고리별_판매량.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_카테고리별_판매량.xlsx'
        gender_file = f'{base_path}/{year}/{year}_성별_매출.xlsx' if year and year.lower() != "all" else f'{base_path}/성별별_판매량.xlsx'
        age_file = f'{base_path}/{year}/{year}_나이대별_판매량.xlsx' if year and year.lower() != "all" else f'{base_path}/나이대별_판매량.xlsx'
        vip_file = f'{base_path}/{year}/{year}_VIP_유저.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_VIP_유저.xlsx'
        area_file = f'{base_path}/{year}/{year}_지역별_판매량.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_지역별_판매량.xlsx'

        summary_data = []

        # 1. Summary 파일 처리
        if os.path.exists(summary_files):
            summary_df = pd.read_excel(summary_files)
            # '년도' 열이 있는 경우 문자열로 변환하고 공백 제거
            if '년도' in summary_df.columns:
                summary_df['년도'] = summary_df['년도'].astype(str).str.strip()

            if year and year.lower() == "all":
                # "전체" 행 추가
                total_sale = summary_df['매출'].sum()
                total_cost = summary_df['판관비'].sum()
                total_net_profit = summary_df['당기순이익'].sum()
                total_row = {
                    '년도': '전체',
                    '매출': total_sale,
                    '판관비': total_cost,
                    '당기순이익': total_net_profit,
                }
                summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)
                # **"전체" 행만 유지**
                summary_df = summary_df[summary_df['년도'] == '전체']
                print("'Summary' 섹션에서 '전체' 행 추가 및 필터링 완료.")
            else:
                print("'Summary' 섹션에서 연도별 데이터 유지.")

            summary_data.append(summary_df)
        else:
            raise FileNotFoundError(f"{summary_files} not found.")

        # 병합 로직: axis=1로 병합 시 각 DataFrame의 인덱스가 동일해야 함
        merged_df = pd.concat(summary_data, axis=1)

        # 중복된 '년도' 열 제거 (첫 번째 '년도' 열만 유지)
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

        # 열 이름 설정
        merged_df.columns = ['년도', '매출', '판관비', '당기순이익']

        # 특정 연도 필터링
        if year and year.lower() != "all":
            filtered_df = merged_df[merged_df['년도'] == str(year).strip()]
            print(f"특정 연도({year})에 대한 데이터 필터링 완료.")
        else:
            filtered_df = merged_df
            print(f"전체 연도에 대한 데이터 유지.")

        summary_json = filtered_df.to_dict(orient='records')
        # 병합 로직: axis=1로 병합 시 각 DataFrame의 인덱스가 동일해야 함
        merged_df = pd.concat(summary_data, axis=1)

        # 중복된 '년도' 열 제거 (첫 번째 '년도' 열만 유지)
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

        # 열 이름 설정
        merged_df.columns = ['년도', '매출', '판관비', '당기순이익']

        # 특정 연도 필터링
        if year and year.lower() != "all":
            filtered_df = merged_df[merged_df['년도'] == str(year).strip()]
            print(f"특정 연도({year})에 대한 데이터 필터링 완료.")
        else:
            filtered_df = merged_df
            print(f"전체 연도에 대한 데이터 유지.")

        # JSON 변환
        summary_json = filtered_df.to_dict(orient='records')

        # 카테고리 데이터 처리
        if os.path.exists(category_file):
            category_df = pd.read_excel(category_file)
            # '년도' 열이 있는 경우 문자열로 변환하고 공백 제거
            if '년도' in category_df.columns:
                category_df['년도'] = category_df['년도'].astype(str).str.strip()

            if year and year.lower() == "all":
                # "전체" 행 추가
                total_sum = category_df['공급가액'].sum()
                total_row = {
                    '카테고리': '전체',
                    '공급가액': total_sum
                }
                category_df = pd.concat([category_df, pd.DataFrame([total_row])], ignore_index=True)
                # **"전체" 행만 유지**
                category_df = category_df[category_df['카테고리'] == '전체']
                print("'카테고리별_판매량' 섹션에서 '전체' 행 추가 및 필터링 완료.")
            else:
                print("'카테고리별_판매량' 섹션에서 연도별 데이터 유지.")

            category_json = category_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{category_file} not found.")

        # 성별 데이터 처리
        if os.path.exists(gender_file):
            gender_df = pd.read_excel(gender_file)
            # '년도' 열이 있는 경우 문자열로 변환하고 공백 제거
            if '년도' in gender_df.columns:
                gender_df['년도'] = gender_df['년도'].astype(str).str.strip()

            if year and year.lower() == "all":
                # "전체" 행 추가
                total_sum = gender_df['공급가액'].sum()
                total_row = {
                    '성별': '전체',
                    '공급가액': total_sum
                }
                gender_df = pd.concat([gender_df, pd.DataFrame([total_row])], ignore_index=True)
                # **"전체" 행만 유지**
                gender_df = gender_df[gender_df['성별'] == '전체']
                print("'성별별_판매량' 섹션에서 '전체' 행 추가 및 필터링 완료.")
            else:
                print("'성별별_판매량' 섹션에서 연도별 데이터 유지.")

            gender_json = gender_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{gender_file} not found.")

        # 나이대 데이터 처리
        if os.path.exists(age_file):
            age_df = pd.read_excel(age_file)
            # '년도' 열이 있는 경우 문자열로 변환하고 공백 제거
            if '년도' in age_df.columns:
                age_df['년도'] = age_df['년도'].astype(str).str.strip()

            if year and year.lower() == "all":
                # "전체" 행 추가
                total_sum = age_df['공급가액'].sum()
                total_row = {
                    '나이대': '전체',
                    '공급가액': total_sum
                }
                age_df = pd.concat([age_df, pd.DataFrame([total_row])], ignore_index=True)
                # **"전체" 행만 유지**
                age_df = age_df[age_df['나이대'] == '전체']
                print("'나이대별_판매량' 섹션에서 '전체' 행 추가 및 필터링 완료.")
            else:
                print("'나이대별_판매량' 섹션에서 연도별 데이터 유지.")

            age_json = age_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{age_file} not found.")

        # VIP 유저 데이터 처리
        if os.path.exists(vip_file):
            vip_df = pd.read_excel(vip_file)
            # '년도' 열이 있는 경우 문자열로 변환하고 공백 제거
            if '년도' in vip_df.columns:
                vip_df['년도'] = vip_df['년도'].astype(str).str.strip()

            # VIP 유저 분석
            vip_df = vip_df.sort_values(by='공급가액', ascending=False).reset_index(drop=True)
            vip_df['누적금액'] = vip_df['공급가액'].cumsum()

            if year and year.lower() == "all":
                # "전체" 행 추가
                total_sum = vip_df['공급가액'].sum()
                total_cumsum = vip_df['공급가액'].sum()  # '누적금액'은 총합과 동일
                total_row = {
                    '연도': '전체',
                    '비율': '전체',  # 비율을 의미 있게 설정하거나 제거
                    '공급가액': total_sum,
                    '누적금액': total_cumsum
                }
                vip_df = pd.concat([vip_df, pd.DataFrame([total_row])], ignore_index=True)
                # **"전체" 행만 유지**
                vip_df = vip_df[vip_df['연도'] == '전체']
                print("'VIP_유저' 섹션에서 '전체' 행 추가 및 필터링 완료.")
            else:
                print("'VIP_유저' 섹션에서 연도별 데이터 유지.")

            # JSON 변환: 필요한 열만 포함
            vip_users_json = vip_df[['연도', '비율', '공급가액', '누적금액']].to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{vip_file} not found.")

        # 지역 데이터 처리
        if os.path.exists(area_file):
            area_df = pd.read_excel(area_file)
            # '년도' 열이 있는 경우 문자열로 변환하고 공백 제거
            if '년도' in area_df.columns:
                area_df['년도'] = area_df['년도'].astype(str).str.strip()

            if year and year.lower() == "all":
                # "전체" 행 추가
                total_sum = area_df['공급가액'].sum()
                total_row = {
                    '지역': '전체',
                    '공급가액': total_sum
                }
                area_df = pd.concat([area_df, pd.DataFrame([total_row])], ignore_index=True)
                # **"전체" 행만 유지**
                area_df = area_df[area_df['지역'] == '전체']
                print("'지역별_판매량' 섹션에서 '전체' 행 추가 및 필터링 완료.")
            else:
                print("'지역별_판매량' 섹션에서 연도별 데이터 유지.")

            area_json = area_df.to_dict(orient='records')
        else:
            raise FileNotFoundError(f"{area_file} not found.")

        # 최종 결과 JSON 생성
        final_json = {
            "summary": summary_json,
            "category_sales": category_json,
            "gender_sales": gender_json,
            "age_group_sales": age_json,
            "vip_sales": vip_users_json,  # VIP 유저 데이터를 JSON에 포함
            "area_sales": area_json  # 지역별 판매 데이터를 JSON에 포함
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