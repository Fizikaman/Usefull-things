import logging
import os

import numpy as np
import pandas as pd
from django.contrib.postgres.search import TrigramSimilarity
from django.core.exceptions import FieldError
from django.db.utils import IntegrityError, DataError
from pandas import DataFrame

from wiki.models import RawMaterial, AttributeValue, Company, Attribute, AttributeName

logger = logging.getLogger(__name__)


class LoadingRawMaterialFromExcel:
    """Класс для загрузки каталогов поставщика из Эксель"""

    def __init__(self, path_file: str):
        """
        Инициализирует экземпляр класса LoadingRawMaterialFromExcel.

        Args:
            path_file: str - путь хранения эксель файла.

        Returns:
                None
        """
        self.path_file = path_file

    def read_excel(self) -> DataFrame:
        """
        Чтение из эксель файла данных в DataFrame.

        Args:
            self
        Returns:
            Возвращает DataFrame с данными о сырье производителя.
        Exception:
            Отлавливаются все ошибки свазнные с открытием или чтением эксель файла.
        """
        filename = os.path.basename(self.path_file)
        try:
            df = pd.read_excel(
                self.path_file, sheet_name=0, dtype=str, na_values=["", " ", "\t", "\n"]
            )
            df = df.replace({np.nan: None})
            logger.info(f"Прочитан эксель файл {filename}")
            return df
        except Exception as e:
            logger.debug(f"При открытии эксель файла {filename} произошла ошибка {e}")

    @staticmethod
    def is_exist_company(company_name):
        """Проверка существования компании в БД"""
        logger.info(f"Ищем компанию: {company_name}")
        try:
            company = (
                Company.objects.annotate(
                    similarity=TrigramSimilarity("name", company_name)
                )
                .filter(similarity__gt=0.8)
                .first()
            )
            return company
        except FieldError:
            logger.debug(f"Компания {company_name} не найдена!")
            return None

    @staticmethod
    def is_exist_raw_material(raw_material_name):
        """Проверка существования сырья в БД"""
        logger.info(f"Ищем сырье: {raw_material_name}")
        try:
            raw_material = (
                RawMaterial.objects.annotate(
                    similarity=TrigramSimilarity("name", raw_material_name)
                )
                .filter(similarity__gt=0.8)
                .first()
            )
            return raw_material
        except FieldError:
            logger.debug(f"Сырье {raw_material_name} не найдено!")
            return None

    def create_raw_material(self):
        """
        Создание сырья в БД из эксель таблицы

        Args:
            self
        Returns:
            None
        Exception:
            IntegrityError - в случае отсутствия значения атрибута не создаем этот атрибут
        Notes:
            Главный метод, который парсит эксель файл. Проверяет существование компании в БД. Если нет, то создает.
            Проверяет наличие сырья. Если нет, то создает.
            Проверка совпадения идет через функцию TrigramSimilarity с процентом совпадения 80.
            Для каждого сырья создается атрибут, где значения или ищутся в БД (100% совпадение) или создаются новые.
        """
        loaded_rm = self.read_excel()
        rm_cnt = loaded_rm.shape[0]
        logger.info(f"Загружено {rm_cnt} единиц сырья.")
        for i, row in loaded_rm.iterrows():
            rm_dict = row.to_dict()

            name_rm = rm_dict["name"]
            description_rm = rm_dict["description"]
            company_rm = rm_dict["company"]

            company_name = self.is_exist_company(company_rm)

            if company_name is None:
                company_rm = Company.objects.create(name=company_rm)
                logger.info(f"Компания {company_rm} не найдена в БД и была создана.")
            else:
                company_rm = company_name

            if self.is_exist_raw_material(name_rm) is None:
                logger.info(f"Сырье {name_rm} не найдено в БД и было создано.")
                name_rm = RawMaterial.objects.create(
                    name=name_rm,
                    description=description_rm,
                    company=company_rm,
                )

                keys_to_remove = ["name", "description", "company"]
                for key in keys_to_remove:
                    del rm_dict[key]

                for key, values in rm_dict.items():
                    atr_name = AttributeName.objects.filter(name=key).first()
                    atr_values = values.split(",") if values is not None else None
                    values_list = []
                    try:
                        for value in atr_values:
                            atr_value, created = AttributeValue.objects.get_or_create(
                                value=value.strip(),
                                attribute_name=atr_name,
                            )
                            values_list.append(atr_value)
                    except (IntegrityError, TypeError):
                        logger.error(
                            f"Значение атрибута {key} None для сырья {name_rm}!"
                        )
                    except DataError:
                        logger.error(
                            f"Значение атрибута {key} для сырья {name_rm} превышает 400 символов!"
                        )
                    Attribute.objects.create(
                        raw_material=name_rm,
                        attribute_name=atr_name,
                    ).attribute_values.add(*values_list)

            logger.info(f"Сырье {name_rm} уже существует в БД.")


def test(path):
    """Для локальных тестов"""
    load_pr = LoadingRawMaterialFromExcel(path)
    load_pr.create_raw_material()
