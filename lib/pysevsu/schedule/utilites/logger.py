import functools
import logging
import time

# Настраиваем логгер (можно настроить по своему усмотрению)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log(func):
    """Декоратор для расширенного логирования вызовов функций."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logging.info(f"Вызов {func.__name__}({signature})")
        try:
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            logging.info(f"{func.__name__} завершена успешно за {elapsed_time:.4f} сек. Результат: {result!r}")
            return result
        except Exception as e:
            elapsed_time = time.time() - start_time
            logging.exception(f"Исключение в {func.__name__} после {elapsed_time:.4f} сек: {e}")
            raise
    return wrapper