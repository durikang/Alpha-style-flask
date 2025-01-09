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
