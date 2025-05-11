import json
import psycopg2
import os

# Thông tin kết nối đến database PostgreSQL
DB_HOST = "localhost"
DB_NAME = "thinh_test"  # Thay thế bằng tên database của bạn
DB_USER = "postgres"      # Thay thế bằng username PostgreSQL của bạn
DB_PASSWORD = "*****"  # Thay thế bằng password PostgreSQL của bạn

# Tên thư mục chứa các file JSON
JSON_FOLDER = "/home/thinh/PycharmProjects/PythonProject/tiki_product_details_by_file_threaded/products_1"  # Thay thế bằng đường dẫn thực tế đến thư mục của bạn

# Tên table bạn muốn lưu dữ liệu vào
TABLE_NAME = "products_1"  # Thay thế bằng tên table của bạn

def connect_to_db():
    """Kết nối đến database PostgreSQL."""
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        return conn
    except psycopg2.Error as e:
        print(f"Lỗi kết nối database: {e}")
        return None

def insert_data(conn, data):
    """Chèn dữ liệu từ một dictionary vào table."""
    cursor = conn.cursor()
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
    try:
        cursor.execute(query, list(data.values()))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Lỗi khi chèn dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()

def process_json_files_from_folder(folder_path):
    """Đọc và xử lý tất cả các file JSON trong một thư mục."""
    conn = connect_to_db()
    if conn:
        json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
        if not json_files:
            print(f"Không tìm thấy file JSON nào trong thư mục: {folder_path}")
            conn.close()
            return

        for filename in json_files:
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    insert_data(conn, data)
                    print(f"Đã lưu dữ liệu từ file: {filename}")
            except FileNotFoundError:
                print(f"Không tìm thấy file: {file_path}")
            except json.JSONDecodeError:
                print(f"Lỗi giải mã JSON trong file: {filename}")
            except Exception as e:
                print(f"Lỗi không xác định khi xử lý file {filename}: {e}")
        conn.close()
        print("Đã hoàn thành việc lưu dữ liệu từ thư mục vào database.")
    else:
        print("Không thể kết nối đến database, dừng lại.")

if __name__ == "__main__":
    process_json_files_from_folder(JSON_FOLDER)