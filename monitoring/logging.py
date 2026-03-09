import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def log_pipeline_start():
    logging.info("Pipeline started")

def log_pipeline_end():
    logging.info("Pipeline completed successfully")
