import logging
import os

def get_logger(file_name):
    log_dir="logs"
    os.makedirs(log_dir,exist_ok=True)
    logger=logging.getLogger(file_name)
    logger.setLevel("DEBUG")
    consle_handler=logging.StreamHandler()
    consle_handler.setLevel("DEBUG")
    log_file_path=os.path.join(log_dir,f"{file_name}.log")
    file_handler=logging.FileHandler(log_file_path)
    file_handler.setLevel("DEBUG")
    formater=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    consle_handler.setFormatter(formater)
    file_handler.setFormatter(formater)
    logger.addHandler(consle_handler)
    logger.addHandler(file_handler)
    return logger
    
