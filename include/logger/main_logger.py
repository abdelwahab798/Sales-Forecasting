import logging
import os

Log_folder=os.getenv("APP_LOG_DIR")
def get_logger(file_name):
    logger=logging.getLogger(file_name)
    os.makedirs(Log_folder,exist_ok=True)
    logger.setLevel(logging.DEBUG)
    formater=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    consle_handler=logging.StreamHandler()
    file_handler=logging.FileHandler(os.path.join(Log_folder, f"{file_name}.log"))
    consle_handler.setFormatter(formater)
    file_handler.setFormatter(formater)
    logger.addHandler(consle_handler)
    logger.addHandler(file_handler)
    return logger
    
