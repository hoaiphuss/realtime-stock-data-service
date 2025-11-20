### Cache trong Map
- Map là CTDL in-memory trong tiến trình NodeJS.
- Dữ liệu được giữ trong heap memory (của tiến trình V8 - bộ nhớ Ram của process)
- Không được ghi ra đĩa, không tồn tại khi process bị restart hoặc server bị tắt.
- Nếu chạy nhiều instance (ví dụ scaling, Docker, k8s với nhiều pod), mỗi instance có Map riêng - không chia sẻ dữ liệu cache giữa các instance.

### Hệ quả và hạn chế
- Mất dữ liệu khi restart: cache bị mất khi process chết/restart/deploy.
- Không chia sẻ trên nhiều node: nếu app horizontal scale, cache không đồng bộ giữa các instance.
- Rủi ro memory leak: nếu map lớn (nhiều key, data to), sẽ tiêu tốn bộ nhớ, có thể gây OOM nếu không giới hạn.
- TTL không chính xác ngay lập tức: vì get không kiểm tra TTL, dữ liệu có thể được trả về sau khi expiration cho đến khi cron chạy xóa.
- Lock / concurrency: Node.js single-threaded nên thao tác Map là an toàn trong cùng một tiến trình; nhưng trong môi trường worker threads hoặc cluster, cần chú ý.

### Để xuất cải tiến theo Map
- Giới hạn kích thước cache: thêm max entries, hoặc dùng một cấu trúc LRU (least-recently-used) để tự dọn khi vượt size.
- Dùng giải pháp distributed cache (nếu cần chia sẻ hoặc bền vững): Redis, Memcached - sẽ tồn tại qua restart và chia sẻ giữa nhiều instance.
- Theo dõi memory/metrics: expose gauge cho số entry, memory usage để giám sát.
- Lược đồ serialization nếu muốn persist: lưu xuống file/DB (thường không khuyến nghị cho cache nóng).
- Thay cron bằng cleanup theo sự kiện: ngoài cron, có thể xóa lazy lúc get (như đề xuất) để ít phụ thuộc cron frequency.
- Sử dụng WeakMap chỉ khi keys là object và bạn muốn cho phép GC tự thu hồi — ít dùng cho string keys.