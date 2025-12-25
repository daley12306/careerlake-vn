import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# ==========================
# Config (giữ giống jobs_silver)
# ==========================
SILVER_TABLE = "nessie.silver.jobs"
EMBED_MODEL_NAME = "/opt/models/bge-m3"

def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("migrate_reclassify_category")
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse")
        .getOrCreate()
    )
    return spark


# ==========================
# CATEGORY 22 ngành chuẩn
# ==========================
CATEGORY_NAMES = [
    "NÔNG NGHIỆP, LÂM NGHIỆP VÀ THỦY SẢN",
    "KHAI KHOÁNG",
    "CÔNG NGHIỆP CHẾ BIẾN, CHẾ TẠO",
    "SẢN XUẤT VÀ PHÂN PHỐI ĐIỆN, KHÍ ĐỐT, NƯỚC NÓNG, HƠI NƯỚC VÀ ĐIỀU HOÀ KHÔNG KHÍ",
    "CUNG CẤP NƯỚC; HOẠT ĐỘNG QUẢN LÝ VÀ XỬ LÝ RÁC THẢI, NƯỚC THẢI",
    "XÂY DỰNG",
    "BÁN BUÔN VÀ BÁN LẺ",
    "VẬN TẢI, KHO BÃI",
    "DỊCH VỤ LƯU TRÚ VÀ ĂN UỐNG",
    "HOẠT ĐỘNG XUẤT BẢN, PHÁT SÓNG, SẢN XUẤT VÀ PHÂN PHỐI NỘI DUNG",
    "HOẠT ĐỘNG VIỄN THÔNG; LẬP TRÌNH MÁY TÍNH, TƯ VẤN, CƠ SỞ HẠ TẦNG MÁY TÍNH VÀ CÁC DỊCH VỤ THÔNG TIN KHÁC",
    "HOẠT ĐỘNG TÀI CHÍNH, NGÂN HÀNG VÀ BẢO HIỂM",
    "HOẠT ĐỘNG KINH DOANH BẤT ĐỘNG SẢN",
    "HOẠT ĐỘNG CHUYÊN MÔN, KHOA HỌC VÀ CÔNG NGHỆ",
    "HOẠT ĐỘNG HÀNH CHÍNH VÀ DỊCH VỤ HỖ TRỢ",
    "HOẠT ĐỘNG CỦA ĐẢNG CỘNG SẢN, TỔ CHỨC CHÍNH TRỊ - XÃ HỘI, QUẢN LÝ NHÀ NƯỚC, AN NINH QUỐC PHÒNG; BẢO ĐẢM XÃ HỘI BẮT BUỘC",
    "GIÁO DỤC VÀ ĐÀO TẠO",
    "Y TẾ VÀ HOẠT ĐỘNG TRỢ GIÚP XÃ HỘI",
    "NGHỆ THUẬT, THỂ THAO VÀ GIẢI TRÍ",
    "HOẠT ĐỘNG DỊCH VỤ KHÁC",
    "HOẠT ĐỘNG LÀM THUÊ CÁC CÔNG VIỆC TRONG CÁC HỘ GIA ĐÌNH, SẢN XUẤT SẢN PHẨM VẬT CHẤT VÀ DỊCH VỤ TỰ TIÊU DÙNG CỦA HỘ GIA ĐÌNH",
    "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ",
]

CATEGORY_VERBOSE = {
    "NÔNG NGHIỆP, LÂM NGHIỆP VÀ THỦY SẢN": "Nông nghiệp, trồng trọt, chăn nuôi, thủy sản, thú y, trang trại, giống cây trồng.",
    "KHAI KHOÁNG": "Khai khoáng, khai thác mỏ, khoáng sản, than, đá, quặng, dầu khí, địa chất, an toàn mỏ.",
    "CÔNG NGHIỆP CHẾ BIẾN, CHẾ TẠO": "Sản xuất, nhà máy, chế tạo, QA/QC, cơ khí, điện-điện tử, lắp ráp, bảo trì, CNC, dệt may, thực phẩm.",
    "SẢN XUẤT VÀ PHÂN PHỐI ĐIỆN, KHÍ ĐỐT, NƯỚC NÓNG, HƠI NƯỚC VÀ ĐIỀU HOÀ KHÔNG KHÍ": "Điện lực, nhà máy điện, trạm biến áp, lưới điện, khí đốt, nồi hơi, HVAC, điều hoà công nghiệp.",
    "CUNG CẤP NƯỚC; HOẠT ĐỘNG QUẢN LÝ VÀ XỬ LÝ RÁC THẢI, NƯỚC THẢI": "Cấp nước, xử lý nước, xử lý nước thải, rác thải, môi trường, quan trắc, HSE.",
    "XÂY DỰNG": "Xây dựng, thi công, công trình, giám sát, QS, kiến trúc, kết cấu, MEP, dự toán, an toàn công trường.",
    "BÁN BUÔN VÀ BÁN LẺ": "Bán buôn, bán lẻ, sales, tư vấn bán hàng, cửa hàng, siêu thị, POS, quản lý cửa hàng, FMCG.",
    "VẬN TẢI, KHO BÃI": "Logistics, vận tải, kho bãi, xuất nhập khẩu, giao nhận, điều phối, hải quan, chứng từ, supply chain.",
    "DỊCH VỤ LƯU TRÚ VÀ ĂN UỐNG": "Nhà hàng, khách sạn, resort, F&B, lễ tân, phục vụ, bếp, buồng phòng, đặt phòng.",
    "HOẠT ĐỘNG XUẤT BẢN, PHÁT SÓNG, SẢN XUẤT VÀ PHÂN PHỐI NỘI DUNG": "Xuất bản, báo chí, truyền thông, phát sóng, biên tập, phóng viên, quay dựng, hậu kỳ, studio.",
    "HOẠT ĐỘNG VIỄN THÔNG; LẬP TRÌNH MÁY TÍNH, TƯ VẤN, CƠ SỞ HẠ TẦNG MÁY TÍNH VÀ CÁC DỊCH VỤ THÔNG TIN KHÁC": "CNTT, lập trình, developer, QA, tester, data, AI, system, network, cloud, viễn thông, IT support.",
    "HOẠT ĐỘNG TÀI CHÍNH, NGÂN HÀNG VÀ BẢO HIỂM": "Ngân hàng, tài chính, kế toán, kiểm toán, tín dụng, bảo hiểm, chứng khoán, đầu tư.",
    "HOẠT ĐỘNG KINH DOANH BẤT ĐỘNG SẢN": "Bất động sản, môi giới, cho thuê, dự án, pháp lý BĐS, sales BĐS.",
    "HOẠT ĐỘNG CHUYÊN MÔN, KHOA HỌC VÀ CÔNG NGHỆ": "Tư vấn, consulting, nghiên cứu, R&D, kiểm định, phòng thí nghiệm, luật, sở hữu trí tuệ.",
    "HOẠT ĐỘNG HÀNH CHÍNH VÀ DỊCH VỤ HỖ TRỢ": "Hành chính, nhân sự, tuyển dụng, C&B, trợ lý, lễ tân, call center, outsourcing, bảo vệ.",
    "HOẠT ĐỘNG CỦA ĐẢNG CỘNG SẢN, TỔ CHỨC CHÍNH TRỊ - XÃ HỘI, QUẢN LÝ NHÀ NƯỚC, AN NINH QUỐC PHÒNG; BẢO ĐẢM XÃ HỘI BẮT BUỘC": "Cơ quan nhà nước, hành chính công, quản lý nhà nước, an ninh quốc phòng, dịch vụ công, BHXH.",
    "GIÁO DỤC VÀ ĐÀO TẠO": "Giáo dục, đào tạo, giáo viên, giảng viên, trợ giảng, e-learning, huấn luyện.",
    "Y TẾ VÀ HOẠT ĐỘNG TRỢ GIÚP XÃ HỘI": "Y tế, bệnh viện, phòng khám, bác sĩ, điều dưỡng, dược sĩ, dược phẩm, thiết bị y tế, xét nghiệm.",
    "NGHỆ THUẬT, THỂ THAO VÀ GIẢI TRÍ": "Nghệ thuật, giải trí, thể thao, sự kiện, biểu diễn, sáng tạo, producer.",
    "HOẠT ĐỘNG DỊCH VỤ KHÁC": "Dịch vụ khác, sửa chữa, chăm sóc cá nhân, làm đẹp, spa, salon, nghề khác.",
    "HOẠT ĐỘNG LÀM THUÊ CÁC CÔNG VIỆC TRONG CÁC HỘ GIA ĐÌNH, SẢN XUẤT SẢN PHẨM VẬT CHẤT VÀ DỊCH VỤ TỰ TIÊU DÙNG CỦA HỘ GIA ĐÌNH": "Giúp việc, tạp vụ gia đình, chăm sóc người già, chăm sóc trẻ em, lao động trong hộ gia đình.",
    "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ": "Tổ chức quốc tế, NGO, INGO, UN, dự án quốc tế, M&E, grant.",
}

def load_category_embedding_model():
    model_eb = SentenceTransformer(EMBED_MODEL_NAME)
    category_texts = [CATEGORY_VERBOSE[name] for name in CATEGORY_NAMES]
    category_embs = model_eb.encode(
        category_texts,
        convert_to_numpy=True,
        normalize_embeddings=True,
        batch_size=32,
        show_progress_bar=False,
    )
    return model_eb, category_embs

def build_job_text_for_silver(title_clean: str, skills_all, industry_sector: str) -> str:
    title = title_clean or ""
    industry_sector = industry_sector or ""

    # skills_all là array<string> trong spark -> pandas sẽ ra list/None
    if isinstance(skills_all, list):
        skills_str = ", ".join([str(x) for x in skills_all if x is not None])
    else:
        skills_str = ""

    parts = [f"Tieu de: {title}. {title}."]  # weight title
    if industry_sector:
        parts.append(f"Nganh_chuan_rule: {industry_sector}. {industry_sector}.")  # weight industry_sector
    if skills_str:
        parts.append(f"Ky nang: {skills_str}")
    return " ".join(parts).strip()

def migrate_reclassify_category(
    spark: SparkSession,
    threshold: float = 0.52,
    fallback: str = "HOẠT ĐỘNG DỊCH VỤ KHÁC",
    limit_rows: int | None = None,   # set để test nhỏ, ví dụ 2000
    backup: bool = True,
):
    # 1) Load silver
    df = spark.read.table(SILVER_TABLE)

    if limit_rows:
        df = df.limit(int(limit_rows))

    # 2) Backup (khuyến nghị)
    if backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_table = f"{SILVER_TABLE}_bak_{ts}"
        (
            df.write.format("iceberg").mode("overwrite").saveAsTable(backup_table)
        )
        print(f"[OK] Backup created: {backup_table}")

    # 3) Extract minimal columns to pandas (giống style job hiện tại của bạn)
    #    NOTE: nếu table rất lớn, cách này sẽ nặng RAM -> xem phần ghi chú ở cuối.
    df_pd = df.select(
        "link",
        "title_clean",
        "industry_sector",
        "skills_all",
    ).toPandas()

    # 4) Build embeddings & classify
    model_eb, category_embs = load_category_embedding_model()

    df_pd["job_text"] = df_pd.apply(
        lambda r: build_job_text_for_silver(
            r.get("title_clean"),
            r.get("skills_all"),
            r.get("industry_sector"),
        ),
        axis=1
    )

    job_embs = model_eb.encode(
        df_pd["job_text"].tolist(),
        convert_to_numpy=True,
        normalize_embeddings=True,
        batch_size=32,
        show_progress_bar=True,
    )

    sim_matrix = cosine_similarity(job_embs, category_embs)
    best_idx = sim_matrix.argmax(axis=1)
    best_scores = sim_matrix.max(axis=1)

    df_pd["category_name_emb"] = [CATEGORY_NAMES[i] for i in best_idx]
    df_pd["category_score_emb"] = best_scores

    df_pd["category_name_final_new"] = np.where(
        df_pd["category_score_emb"] >= threshold,
        df_pd["category_name_emb"],
        fallback,
    )

    # 5) Create mapping df (link -> new category)
    df_map = spark.createDataFrame(
        df_pd[["link", "category_name_final_new"]]
        .rename(columns={"category_name_final_new": "category_name_final"})
    )

    # 6) Join back to original df -> update column category_name_final
    #    (giữ nguyên tất cả các cột khác)
    df_updated = (
        df.drop("category_name_final")
          .join(df_map, on="link", how="left")
    )

    # 7) Validate quickly
    print("[INFO] Category distribution (top 30):")
    df_updated.groupBy("category_name_final").count().orderBy(F.desc("count")).show(30, truncate=False)

    # 8) Overwrite back to same table (schema unchanged)
    (
        df_updated
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )
    print(f"[OK] Migrated and overwritten table: {SILVER_TABLE}")


def main():
    spark = build_spark_session()

    # ENV vars (optional)
    threshold = float(os.getenv("MIGRATE_THRESHOLD", "0.52"))
    limit_rows = os.getenv("MIGRATE_LIMIT")
    limit_rows = int(limit_rows) if limit_rows else None
    backup = os.getenv("MIGRATE_BACKUP", "1") == "1"

    migrate_reclassify_category(
        spark=spark,
        threshold=threshold,
        fallback="HOẠT ĐỘNG DỊCH VỤ KHÁC",
        limit_rows=limit_rows,
        backup=backup,
    )

    spark.stop()

if __name__ == "__main__":
    main()
