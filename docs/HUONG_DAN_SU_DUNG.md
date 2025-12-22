# HƯỚNG DẪN SỬ DỤNG BÁO CÁO OVERLEAF

## 1. TẠO CÁC DIAGRAMS

### Trang web để tạo diagrams: https://www.plantuml.com

### Các hình cần tạo:

#### Hình 3.1: Kiến trúc hệ thống tổng thể
**Tên file:** `architecture_diagram.png`

1. Truy cập https://www.plantuml.com
2. Copy mã PlantUML từ file `baocao.tex` (phần Hình 3.1, từ @startuml đến @enduml)
3. Dán vào editor trên trang web
4. Chọn format "PNG" và click "Submit"
5. Download hình và lưu với tên: `architecture_diagram.png`
6. Upload vào Overleaf trong thư mục `docs/`

#### Hình 3.2: Flow xử lý dữ liệu
**Tên file:** `data_flow_diagram.png`

1. Truy cập https://www.plantuml.com
2. Copy mã PlantUML từ file `baocao.tex` (phần Hình 3.2, từ @startuml đến @enduml)
3. Dán vào editor trên trang web
4. Chọn format "PNG" và click "Submit"
5. Download hình và lưu với tên: `data_flow_diagram.png`
6. Upload vào Overleaf trong thư mục `docs/`

#### Hình 3.3: Airflow DAG Dependencies
**Tên file:** `dag_dependencies.png`

1. Truy cập https://www.plantuml.com
2. Copy mã PlantUML từ file `baocao.tex` (phần Hình 3.3, từ @startuml đến @enduml)
3. Dán vào editor trên trang web
4. Chọn format "PNG" và click "Submit"
5. Download hình và lưu với tên: `dag_dependencies.png`
6. Upload vào Overleaf trong thư mục `docs/`

#### Hình 3.4: Kafka Topics Flow
**Tên file:** `kafka_flow.png`

1. Truy cập https://www.plantuml.com
2. Copy mã PlantUML từ file `baocao.tex` (phần Hình 3.4, từ @startuml đến @enduml)
3. Dán vào editor trên trang web
4. Chọn format "PNG" và click "Submit"
5. Download hình và lưu với tên: `kafka_flow.png`
6. Upload vào Overleaf trong thư mục `docs/`

#### Hình 3.5: Spark ML Pipeline
**Tên file:** `spark_ml_pipeline.png`

1. Truy cập https://www.plantuml.com
2. Copy mã PlantUML từ file `baocao.tex` (phần Hình 3.5, từ @startuml đến @enduml)
3. Dán vào editor trên trang web
4. Chọn format "PNG" và click "Submit"
5. Download hình và lưu với tên: `spark_ml_pipeline.png`
6. Upload vào Overleaf trong thư mục `docs/`

## 2. CHÈN HÌNH ẢNH VÀO OVERLEAF

### Các hình ảnh đã có sẵn:

1. **sparkonl.jpg** - Hình 4.1: Spark UI
   - Đã có sẵn trong folder `docs/`
   - Tên trong LaTeX: `sparkonl.jpg`

2. **airlowonl.jpg** - Hình 4.2: Airflow UI
   - Đã có sẵn trong folder `docs/`
   - Tên trong LaTeX: `airlowonl.jpg`

3. **hadoopsave.jpg** - Hình 4.3: Model saved trên HDFS
   - Đã có sẵn trong folder `docs/`
   - Tên trong LaTeX: `hadoopsave.jpg`

### Các hình cần tạo từ PlantUML:

1. **architecture_diagram.png** - Hình 3.1
2. **data_flow_diagram.png** - Hình 3.2
3. **dag_dependencies.png** - Hình 3.3
4. **kafka_flow.png** - Hình 3.4
5. **spark_ml_pipeline.png** - Hình 3.5

## 3. CÁCH UPLOAD VÀ SỬ DỤNG TRONG OVERLEAF

### Bước 1: Upload hình ảnh
1. Trong Overleaf, click vào icon "Upload" ở sidebar bên trái
2. Upload tất cả các file hình ảnh vào project
3. Đảm bảo các file có đúng tên như đã liệt kê ở trên

### Bước 2: Kiểm tra đường dẫn
- Trong file `baocao.tex`, các hình được reference với tên file trực tiếp
- Ví dụ: `\includegraphics[width=0.9\textwidth]{sparkonl.jpg}`
- Overleaf sẽ tự động tìm file trong cùng thư mục với file `.tex`

### Bước 3: Compile
1. Click "Recompile" trong Overleaf
2. Kiểm tra xem tất cả hình đã hiển thị đúng chưa
3. Nếu có lỗi, kiểm tra lại tên file và đường dẫn

## 4. LƯU Ý QUAN TRỌNG

### Về các hình Reference:
- **KHÔNG** sử dụng các file:
  - `Reference_images.png`
  - `Reference_images1.png`
  - `Reference_images2.png`
- Đây là hình của nhóm khác, không phải của dự án này

### Về chất lượng hình:
- Các hình từ PlantUML nên export ở độ phân giải cao (PNG)
- Có thể chỉnh sửa kích thước trong LaTeX bằng tham số `width`

### Về vị trí hình:
- Tất cả hình đều sử dụng `[H]` placement để đảm bảo hình xuất hiện đúng vị trí
- Nếu hình quá lớn, có thể điều chỉnh `width` trong `\includegraphics`

## 5. CHECKLIST TRƯỚC KHI NỘP

- [ ] Đã tạo đủ 5 hình từ PlantUML
- [ ] Đã upload tất cả hình vào Overleaf
- [ ] Đã kiểm tra tên file khớp với code LaTeX
- [ ] Đã compile thành công không có lỗi
- [ ] Tất cả hình đều hiển thị đúng
- [ ] Đã kiểm tra chất lượng hình ảnh
- [ ] Đã xóa các hình Reference không cần thiết

## 6. TROUBLESHOOTING

### Lỗi: "File not found"
- Kiểm tra lại tên file có đúng không
- Kiểm tra file đã được upload vào Overleaf chưa
- Kiểm tra đường dẫn trong `\includegraphics`

### Lỗi: Hình quá lớn/nhỏ
- Điều chỉnh tham số `width` trong `\includegraphics`
- Ví dụ: `width=0.8\textwidth` hoặc `width=0.9\textwidth`

### Lỗi: Hình không hiển thị
- Kiểm tra format file (phải là .jpg, .png, .pdf)
- Kiểm tra file có bị corrupt không
- Thử recompile lại

