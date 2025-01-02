import pandas as pd


def preprocess_data(file_path):
    """
    데이터를 전처리하고 필터링된 결과를 반환하는 함수.

    Args:
        file_path (str): 엑셀 파일 경로

    Returns:
        pd.DataFrame: 전처리된 데이터프레임
    """
    print(f"Reading merged file from: {file_path}")
    merged_data = pd.read_excel(file_path)

    print("Preprocessing data...")
    required_columns = ['유저번호', '년도', '월', '일', '매입매출구분(1-매출/2-매입)', '공급가액', '판매비와 관리비']
    # 결측치를 0으로 대체
    print("Filling missing values with 0 for required columns...")
    merged_data[required_columns] = merged_data[required_columns].fillna(0)

    for col in ['공급가액', '판매비와 관리비', '수량', '단가', '부가세']:
        merged_data[col] = pd.to_numeric(merged_data[col], errors='coerce').fillna(0)

    # 필터링된 데이터를 생성
    filtered_data = merged_data[(merged_data['공급가액'] > 0) & (merged_data['판매비와 관리비'] >= 0)]

    return filtered_data


# 테스트 실행
if __name__ == "__main__":
    test_file_path = "merged/merged_data.xlsx"  # 테스트할 파일 경로
    filtered_data = preprocess_data(test_file_path)

    # 전처리된 데이터 확인
    print("Filtered Data:")
    print(filtered_data)
