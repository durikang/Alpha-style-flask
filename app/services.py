import pandas as pd
import os

def handle_file_upload(files):
    UPLOAD_FOLDER = './uploads'
    MERGED_FOLDER = './app/merged'

    file_paths = []
    try:
        for file in files:
            file_path = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(file_path)
            file_paths.append(file_path)
            print(f"Saved file: {file_path}")  # 디버깅용 출력

        # 데이터 병합
        dataframes = []
        for path in file_paths:
            print(f"Processing file: {path}")  # 디버깅용 출력
            if path.endswith('.csv'):
                df = pd.read_csv(path)
            elif path.endswith('.xlsx'):
                df = pd.read_excel(path)
            else:
                raise ValueError(f"Unsupported file format: {os.path.basename(path)}")
            dataframes.append(df)

        if not dataframes:
            raise ValueError("No valid files to merge.")

        # 병합된 데이터 저장
        merged_df = pd.concat(dataframes, ignore_index=True)
        output_file_path = os.path.join(MERGED_FOLDER, 'merged_data.xlsx')
        merged_df.to_excel(output_file_path, index=False)
        print(f"Merged file saved at: {output_file_path}")  # 디버깅용 출력

        return {'message': 'Files merged and saved successfully', 'output_path': output_file_path}
    except Exception as e:
        print("Error in handle_file_upload:", str(e))  # 예외 디버깅
        raise e  # 에러 재발생
