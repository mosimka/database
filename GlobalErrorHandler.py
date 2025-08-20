import sys
import traceback
from IPython import get_ipython
from . import DB


class GlobalErrorHandler:
    _instance = None
    _error_callbacks = []

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._setup_global_hook()
        return cls._instance

    def _setup_global_hook(self):
        self._original_excepthook = sys.excepthook
        sys.excepthook = self._global_except_hook
        
        ipython = get_ipython()
        if ipython:
            self._original_ipython_excepthook = ipython.showtraceback
            ipython.showtraceback = self._ipython_except_hook

    def _ipython_except_hook(self, exc_tuple=None, filename=None, tb_offset=None,
                           exception_only=False, running_compiled_code=False):
        """Кастомный обработчик для IPython/Spyder, совместимый с оригинальным API"""
        try:
            # Получаем информацию об исключении как в оригинальной реализации
            etype, value, tb = self._get_exc_info(exc_tuple)
            # print("\nIPython/Spyder exception handler triggered!")
            DB.close()
            # Обрабатываем исключение через наш механизм
            tb_str = ''.join(traceback.format_exception(etype, value, tb))
            for callback in self._error_callbacks:
                callback(etype, value, tb_str)
            
            # Вызываем оригинальный обработчик IPython
            return self._original_ipython_excepthook(
                exc_tuple, filename, tb_offset, exception_only, running_compiled_code
            )
            
        except Exception as e:
            print(f"Error in custom exception handler: {e}", file=sys.stderr)
            return self._original_ipython_excepthook(
                exc_tuple, filename, tb_offset, exception_only, running_compiled_code
            )
        
    def _get_exc_info(self, exc_tuple=None):
        """Аналог IPython's _get_exc_info для совместимости"""
        if exc_tuple is None:
            return sys.exc_info()
        elif isinstance(exc_tuple, BaseException):
            return (exc_tuple.__class__, exc_tuple, exc_tuple.__traceback__)
        elif len(exc_tuple) == 3:
            return exc_tuple
        else:
            return sys.exc_info()

    def _global_except_hook(self, exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            return sys.__excepthook__(exc_type, exc_value, exc_traceback)

        tb_str = ''.join(traceback.format_exception(
            exc_type, exc_value, exc_traceback))
        print("GLOBAL")
        print(f"Exception type: {exc_type.__name__}")
        print(f"Exception value: {str(exc_value)}")

        for callback in self._error_callbacks:
            print(
                f"Calling callback: {callback.__name__ if hasattr(callback, '__name__') else callback}")
            callback(exc_type, exc_value, tb_str)

        print("Calling original excepthook")
        sys.__excepthook__(exc_type, exc_value, exc_traceback)


error_handler = GlobalErrorHandler()
if __name__ == "__main__":
    DB.insertObject("hui", False, **{"hui":"mocha"})
