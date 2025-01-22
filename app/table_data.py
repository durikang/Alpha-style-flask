import pandas as pd
import os
import json

def generate_json_from_excel(year=None):
    try:
        # 파일 경로 설정
        base_path = './analysis/xlsx'
        summary_files = f'{base_path}/{year}/{year}_재무지표.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_재무지표.xlsx'
        category_file = f'{base_path}/{year}/{year}_카테고리별_판매량.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_카테고리별_판매량.xlsx'
        gender_file = f'{base_path}/{year}/{year}_성별_매출.xlsx' if year and year.lower() != "all" else f'{base_path}/성별별_판매량.xlsx'
        age_file = f'{base_path}/{year}/{year}_나이대별_판매량.xlsx' if year and year.lower() != "all" else f'{base_path}/나이대별_판매량.xlsx'
        vip_file = f'{base_path}/{year}/{year}_VIP_유저.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_VIP_유저.xlsx'
        area_file = f'{base_path}/{year}/{year}_지역별_판매량.xlsx' if year and year.lower() != "all" else f'{base_path}/연도별_지역별_판매량.xlsx'

        final_json = {}

        # 섹션별 설정
        sections = [
            {
                "name": "summary",
                "file": summary_files,
                "required_columns": ['년도', '매출', '판관비', '당기순이익', '예측매출', '예측판관비', '예측당기순이익'],
                "group_col": '년도',
                "sum_cols": ['매출', '판관비', '당기순이익', '예측매출', '예측판관비', '예측당기순이익'],
                "total_row_template": {'년도': '전체'}
            },
            {
                "name": "category_sales",
                "file": category_file,
                "required_columns": ['카테고리', '실제공급가액', '예측공급가액'],
                "group_col": '카테고리',
                "sum_cols": ['실제공급가액', '예측공급가액'],
                "total_row_template": {'카테고리': '전체'}
            },
            {
                "name": "gender_sales",
                "file": gender_file,
                "required_columns": ['성별', '실제공급가액', '예측공급가액'],
                "group_col": '성별',
                "sum_cols": ['실제공급가액', '예측공급가액'],
                "total_row_template": {'성별': '전체'}
            },
            {
                "name": "age_group_sales",
                "file": age_file,
                "required_columns": ['나이대', '실제공급가액', '예측공급가액'],
                "group_col": '나이대',
                "sum_cols": ['실제공급가액', '예측공급가액'],
                "total_row_template": {'나이대': '전체'}
            },
            {
                "name": "vip_sales",
                "file": vip_file,
                "required_columns": ['비율', '실제공급가액', '예측공급가액'],  # '연도' 제거
                "group_col": '비율',
                "sum_cols": ['실제공급가액', '예측공급가액'],
                "total_row_template": {'비율': '전체'}
            },
            {
                "name": "area_sales",
                "file": area_file,
                "required_columns": ['지역', '실제공급가액', '예측공급가액'],
                "group_col": '지역',
                "sum_cols": ['실제공급가액', '예측공급가액'],
                "total_row_template": {'지역': '전체'}
            },
        ]

        # 열 이름 매핑: 섹션별로 실제공급가액을 나타내는 다른 열 이름을 매핑
        column_mappings = {
            "gender_sales": {
                "공급가액": "실제공급가액"  # '공급가액'을 '실제공급가액'으로 매핑
            },
            "vip_sales": {
                "실제공급가액(억)": "실제공급가액",
                "예측공급가액(억)": "예측공급가액"
            },
            "age_group_sales": {
                "공급가액": "실제공급가액"
            },
            "vip_sales": {  # 중복 제거
                "실제공급가액(억)": "실제공급가액",
                "예측공급가액(억)": "예측공급가액"
            },
            # 'category_sales' 섹션은 이미 '실제공급가액'과 '예측공급가액'을 사용하므로 추가 매핑 필요 없음
        }

        for section in sections:
            name = section["name"]
            file_path = section["file"]
            required_columns = section["required_columns"]
            group_col = section["group_col"]
            sum_cols = section["sum_cols"]
            total_row_template = section["total_row_template"]

            if os.path.exists(file_path):
                try:
                    df = pd.read_excel(file_path)
                    print(f"[DEBUG] Processing file: {file_path}")
                    print(f"[DEBUG] Columns before processing ({name}): {df.columns.tolist()} (총 {len(df.columns)}개)")

                    # 열 이름 표준화: 공백 제거 및 소문자 변환
                    df.columns = df.columns.str.replace(' ', '').str.lower()

                    # 섹션별 열 매핑 적용
                    if name in column_mappings:
                        mappings = column_mappings[name]
                        for original, new in mappings.items():
                            original_std = original.replace(' ', '').lower()
                            new_std = new.replace(' ', '').lower()
                            if original_std in df.columns:
                                df = df.rename(columns={original_std: new_std})

                    # 필요한 열 이름도 표준화
                    standardized_required_columns = [col.replace(' ', '').lower() for col in required_columns]
                    group_col_std = group_col.replace(' ', '').lower()

                    # 데이터프레임 내 필요한 열이 있는지 확인 (예측공급가액이 없을 경우 처리)
                    existing_required_columns = [col for col in standardized_required_columns if col in df.columns]
                    missing_columns = [col for col in standardized_required_columns if col not in df.columns]

                    if missing_columns:
                        print(f"[WARNING] {name} 섹션에 필요한 열이 누락되었습니다: {missing_columns}")
                        # 예측공급가액이 누락된 경우, '예측공급가액'을 'N/A'로 채우기
                        for col in missing_columns:
                            df[col] = 'N/A'
                        # 업데이트된 required_columns 리스트로 변경
                        existing_required_columns = standardized_required_columns

                    # 그룹 열 표준화
                    if group_col_std in df.columns:
                        df[group_col_std] = df[group_col_std].astype(str).str.strip()
                    else:
                        raise ValueError(f"{name} 섹션에서 그룹 열 '{group_col}'을(를) 찾을 수 없습니다.")

                    if year and year.lower() == "all":
                        # "전체" 행 추가
                        total_sums = {}
                        for col in sum_cols:
                            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                                total_sums[col] = df[col].sum()
                            else:
                                # '예측공급가액'이 'N/A'인 경우 처리
                                total_sums[col] = 'N/A'
                        total_row = {**total_row_template, **total_sums}
                        df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
                        # "전체" 행만 유지
                        df = df[df[group_col_std] == '전체']
                        print(f"[DEBUG] '{name}' 섹션에서 '전체' 행 추가 및 필터링 완료.")
                    else:
                        print(f"[DEBUG] '{name}' 섹션에서 연도별 데이터 유지.")

                    print(f"[DEBUG] Columns before selecting required columns ({name}): {df.columns.tolist()} (총 {len(df.columns)}개)")

                    # 필요한 열만 선택
                    df = df[existing_required_columns]
                    print(f"[DEBUG] Columns after selecting required columns ({name}): {df.columns.tolist()} (총 {len(df.columns)}개)")

                    # JSON 변환
                    final_json[name] = df.to_dict(orient='records')

                except Exception as e:
                    print(f"[ERROR] {name} 섹션 처리 중 오류 발생: {str(e)}")
                    raise
            else:
                print(f"[WARNING] {file_path} 파일을 찾을 수 없습니다. 해당 섹션을 건너뜁니다.")
                continue  # 파일이 없을 경우 건너뜀

        # JSON 저장
        try:
            output_dir = './analysis/json'
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{year if year else '전체'}_분석결과.json")

            with open(output_file, 'w', encoding='utf-8') as json_file:
                json.dump(final_json, json_file, ensure_ascii=False, indent=4)

            print(f"[DEBUG] JSON 파일 저장 완료: {output_file}")
        except Exception as e:
            print(f"[ERROR] JSON 파일 저장 중 오류 발생: {str(e)}")
            raise

        # 결과 반환
        return final_json

    except Exception as e:
        print(f"[ERROR] Exception in generate_json_from_excel: {str(e)}")
        raise
