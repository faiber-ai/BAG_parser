import time
import utils
import locale

locale.setlocale(locale.LC_ALL, 'en_US')

class StatusUpdater:
    last_update_time = None
    start_time = None
    elapsed_time = None
    label = None
    count = 0
    total_count = 0
    refresh_time = 0.5

    def start(self, total_count, label=''):
        self.last_update_time = None
        self.elapsed_time = None
        self.count = 0
        self.total_count = total_count
        self.label = label
        self.start_time = time.perf_counter()
        self.__update_bar(False)

    def __update_bar(self, final, info=''):
        self.last_update_time = time.perf_counter()
        self.elapsed_time = self.last_update_time - self.start_time
        count_per_second = round(self.count / self.elapsed_time)

        time_elapsed = utils.time_elapsed(self.start_time)
        text = f'{time_elapsed} | {self.label} {self.count:,}/{self.total_count:,d} ({count_per_second:,d}/s)'
        if info:
            text += f' | {info}'

        total_count = self.total_count if (self.total_count > 0) else self.count
        utils.print_progress_bar(self.count, total_count, text, final)

    def update(self, count):
        self.count = count
        if ((self.last_update_time is None) or
                (time.perf_counter() - self.last_update_time > self.refresh_time)):
            self.__update_bar(False)

    def end(self, clear=False, info=''):
        if clear:
            print("\r", end='')
        else:
            self.__update_bar(True, info)
