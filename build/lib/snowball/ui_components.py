"""
ui_components.py

UI components and display functions for user interaction.
"""
import os
import sys
import time
import msvcrt
import shutil
from tqdm import tqdm
from config import *


class UIComponents:
    def __init__(self):
        term_width = shutil.get_terminal_size().columns
        self.bar_width = term_width // 4

    def welcome_message(self):
        print("\n")
        message = "Welcome to Snowball Product!"
        width = len(message) + 8
        border = "*" * width

        line1 = border
        line2 = "*" + message.center(width - 2) + "*"
        line3 = border

        term_width = shutil.get_terminal_size().columns

        print(line1.center(term_width))
        print(line2.center(term_width))
        print(line3.center(term_width))

    def rotating_slash_after(self, text, duration_sec=5, passed=1):
        print(text, end=" ", flush=True)
        spinner = ['|', '/', '-', '\\']
        end_time = time.time() + duration_sec
        i = 0
        while time.time() < end_time:
            sys.stdout.write('\r' + text + " " + spinner[i % len(spinner)])
            sys.stdout.flush()
            time.sleep(0.1)
            i += 1
        green_tick = "\u2714"
        green_color = "\033[92m"
        red_cross = "\u274C"
        red_color = "\033[91m"
        reset_color = "\033[0m"
        if passed == 1:
            print_msg = f"{text} {green_color}Done {green_tick}{reset_color}\n"
        else:
            print_msg = f"{text} {red_color}Failed {red_cross}{reset_color}\n"
        sys.stdout.write('\r' + print_msg)
        sys.stdout.flush()

    def blinking_dots_input(self, base_text="Press Enter to continue"):
        dots = ['', '.', '..', '...']
        i = 0
        print(base_text, end='', flush=True)

        while True:
            print('\r' + base_text + dots[i % len(dots)] + '   ', end='', flush=True)
            time.sleep(0.5)
            i += 1

            if msvcrt.kbhit():
                key = msvcrt.getwch()
                if key == '\r':
                    break

    def initial_set_up(self):
        text = "Setting up initial requirements"
        width = len(text) + 8
        border = "*" * width * 2

        line1 = border
        line2 = " " + text.center(width - 2)
        line3 = ("*" * (len(line2) // 3)).center(width - 2)
        line4 = "1. Collecting latest repo from Git "
        line5 = f"2. Column mapping file has been downloaded to {Path.home()}\Downloads\column_mapping.csv with dummy data for reference. "
        line6 = "3. Please update the downloaded Column mapping file as per your revenue data and save it to Downloads"
        line7 = "4. Similar to column mapping, Update your data platform credentials as well in the profiles.yml(which is like .env file) from Downloads"
        line9 = "Press Enter to continue "

        term_width = shutil.get_terminal_size().columns

        print(line1)
        print(line2)
        print(line3)
        self.rotating_slash_after(line4, 5)
        self.rotating_slash_after(line5, 1)
        print(line6)
        print(line7)
        self.blinking_dots_input(line9)
        print("\n")
        print(line1)
        print("\n")

    def show_progress(self, desc, duration=None, steps=None):
        if duration:
            with tqdm(total=100, desc=desc, colour="green", bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}| {elapsed}') as pbar:
                step_time = duration / 100
                for i in range(100):
                    time.sleep(step_time)
                    pbar.update(1)
        elif steps:
            pbar = tqdm(total=steps, desc=desc, bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}| {n_fmt}/{total_fmt}')
            return pbar
        else:
            return tqdm(desc=desc, bar_format='{desc}  Processing...')

    def print_section_header(self, text):
        width = len(text) + 8
        border = "*" * width

        line1 = border
        line2 = " " + text.center(width - 2)
        line3 = ("*" * (len(line2) // 3)).center(width - 2)
        
        print(line1)
        print(line2)
        print(line3)