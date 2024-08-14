# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: selenium.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for making web scraping with Selenium
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from seleniumwire import webdriver


class UtilsSelenium:
    """
    This is the main class that contains all the necessary methods to interact with a web page using Selenium.
    """

    def __init__(self, url: str) -> None:
        """
        Initializes the Selenium class with the URL of the web page you want to interact with.

        Args:
            url (str): The URL of the web page.
        """
        self.url = url

    @staticmethod
    def instantiate_browser_options(browser_option_class: callable) -> any:
        """
        Creates an instance of Options which allows you to set options for the WebDriver.

        Args:
            browser_option_class (str): The name of the browser (e.g."ChromeOptions", "FirefoxOptions")

        Returns:
            WebDriverOptions: Options instance corresponding to the browser.
        """
        return browser_option_class()

    def add_argument_option(self, options: callable, argument: str) -> None:
        """
        Adds an argument to the ChromeOptions instance.

        Args:
            options (callable): The options to be used when creating the Chrome driver.
            argument (str): The argument to be added to ChromeOptions.
        """
        options.add_argument(argument)

    @staticmethod
    def _driver_install(driver: callable):
        """
        Installs a instance of Driver which is necessary to create a new instance.
        Args:
            driver (callable): The installed a Driver.

        Returns:
        """
        return driver().install()

    def instantiate_browser_driver(
        self, webdriver_name: callable, driver_name_install: callable, options: callable
    ):
        """
        Instantiates a Chrome driver with the given options.
        """
        return webdriver_name(
            options=options, service=Service(self._driver_install(driver_name_install))
        )

    def go_to_page(self, driver: webdriver):
        """
        Navigates the driver to the specified URL.

        Args:
            driver (webdriver): The Chrome driver.
        """
        driver.get(self.url)

    @staticmethod
    def find_element(driver: webdriver, search_for: By, element_name: str):
        """
        Finds an element on the page.

        Args:
            driver (webdriver.Chrome): The Chrome driver.
            search_for (By): The method to locate the element by.
            element_name (str): The name of the element to find.

        Returns:
            WebElement: The found element.
        """
        return driver.find_element(search_for, element_name)

    @staticmethod
    def find_elements(driver: webdriver, search_for: By, element_name: str):
        """
        Finds an element on the page.

        Args:
            driver (webdriver.Chrome): The Chrome driver.
            search_for (By): The method to locate the element by.
            element_name (str): The name of the element to find.

        Returns:
            WebElement: The found element.
        """
        return driver.find_elements(search_for, element_name)

    @staticmethod
    def _get_select_tag(element):
        """
        Gets the Select tag from an element.

        Args:
            element (WebElement): The web element.

        Returns:
            Select: The Select tag of the element.
        """
        return Select(element)

    def select_by_value(self, element, value: str) -> None:
        """
        Selects an option from a Select tag by its value.

        Args:
            element (WebElement): The web element.
            value (str): The value of the option to select.
        """
        select_obj = self._get_select_tag(element)
        select_obj.select_by_value(value)

    @staticmethod
    def webdrive_wait(driver: webdriver, wait_for: By, element_name: str) -> None:
        """
        Waits until an element is present on the page.

        Args:
            driver (webdriver.Chrome): The Chrome driver.
            wait_for (By): The method to locate the element by.
            element_name (str): The name of the element to wait for.
        """
        expected = EC.presence_of_element_located((wait_for, element_name))
        WebDriverWait(driver, 10).until(expected)

    @staticmethod
    def execute_script(driver: webdriver, command: str) -> None:
        """
        Executes a JavaScript command.

        Args:
            driver (webdriver.Chrome): The Chrome driver.
            command (str): The JavaScript command to execute.
        """
        driver.execute_script(command)

    @staticmethod
    def close_browser(driver: webdriver) -> None:
        """
        Closes the browser currently being controlled by the webdriver.

        Args:
            driver (webdriver): The instance of the webdriver that is controll
        """
        driver.quit()
