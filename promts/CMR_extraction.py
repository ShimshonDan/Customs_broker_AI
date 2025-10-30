from typing import Dict, Any

CMR_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        # --- A) Обязательное (отгрузка) ---
        "consignor": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "address": {"type": "string"},
                "country": {"type": "string"},
                "vat_or_tax_id": {"type": "string"},
                "contacts": {"type": "string"}
            },
            "required": ["name"]
        },
        "consignee": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "address": {"type": "string"},
                "country": {"type": "string"},
                "vat_or_tax_id": {"type": "string"},
                "contacts": {"type": "string"}
            },
            "required": ["name"]
        },
        "place_and_date_taking_over": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "place": {"type": "string"},
                "date": {
                    "type": "string",
                    "pattern": r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"
                }
            },
            "required": ["place"]
        },
        "place_of_delivery": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "place": {"type": "string"},
                "country": {"type": "string"},
                "address": {"type": "string"}
            },
            "required": ["place"]
        },
        "packages_summary": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "number_of_packages": {"type": "number"},
                "kind_of_packages": {"type": "string"},  # коробки/паллеты и т.п.
                "marks_and_numbers": {"type": "string"}  # общая маркировка/диапазоны
            }
        },
        "gross_weight_total_kg": {"type": "number"},
        "net_weight_total_kg": {"type": "number"},

        # --- B) Транспортные параметры ---
        "transport": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "mode": {"type": "string", "enum": ["road"]},  # дорога
                "mode_code_border": {"type": "string", "enum": ["30"]},  # гр.25
                "mode_code_inland": {"type": "string", "enum": ["30"]},  # гр.26
                "tractor_plate": {"type": "string"},
                "trailer_plate": {"type": "string"},
                "plate_country_code": {"type": "string"},  # ISO 3166-1 alpha-2, если удастся
                "volume_m3": {"type": "number"},
                "seals": {"type": "array", "items": {"type": "string"}}  # номера пломб
            },
            "required": ["mode"]
        },
        "carrier": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "address": {"type": "string"},
                "vat_or_tax_id": {"type": "string"},
                "contacts": {"type": "string"}
            }
        },
        "route_countries": {
            "type": "array",
            "items": {"type": "string"},  # список стран следования, в порядке маршрута
            "description": "Countries en route"
        },

        # --- C) Документооборот/идентификаторы ---
        "cmr_number": {"type": "string"},
        "cmr_series": {"type": "string"},
        "cmr_date": {
            "type": "string",
            "pattern": r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"
        },
        "related_documents": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "type": {"type": "string"},  # Invoice, Packing List, Contract и т.п.
                    "number": {"type": "string"},
                    "date": {
                        "type": "string",
                        "pattern": r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"
                    }
                }
            }
        },

        # --- ITEMS по строкам (если CMR содержит детализацию) ---
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "line_no": {"type": "string"},
                    "description": {"type": "string"},        # краткое наименование → гр.31
                    "quantity": {"type": "number"},
                    "uom": {"type": "string"},
                    "kind_of_packages": {"type": "string"},
                    "marks_and_numbers": {"type": "string"},
                    "gross_weight_kg": {"type": "number"},     # → гр.35 по строке (если есть)
                    "net_weight_kg": {"type": "number"}        # → гр.38 по строке (если есть)
                },
                "required": ["description"]
            }
        },

        # --- D) Необязательное, но полезное ---
        "driver": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "contacts": {"type": "string"},
                "passport_or_id": {"type": "string"}
            }
        },
        "special_instructions": {"type": "string"},   # особые отметки / agreements
        "transshipment": {"type": "string"},          # перегрузка/доп. перевозчики, если указано
        "notes": {"type": "string"}
    },
    "required": [
        "consignor",
        "consignee",
        "place_and_date_taking_over",
        "place_of_delivery",
        "packages_summary",
        "gross_weight_total_kg",
        "transport"
    ]
}

# ===== Подсказка модели (инструкция) =====
CMR_INSTRUCTION_RU = """Вы извлекаете данные из международной автотранспортной накладной CMR.
Верните СТРОГО валидный JSON по переданной JSON Schema. Никаких комментариев/префиксов — только JSON.

Маппинг на графы ДТ:
- consignor → сверка с гр.2; consignee → сверка с гр.8
- place_and_date_taking_over.date → контроль даты отгрузки
- place_of_delivery → страна назначения (для понимания гр.15/17)
- packages_summary.number_of_packages/kind_of_packages + marks_and_numbers → гр.31
- items[].description/marks_and_numbers/kind_of_packages → гр.31 (если строки есть)
- gross_weight_total_kg и items[].gross_weight_kg → гр.35; net_weight_total_kg/items[].net_weight_kg → гр.38
- transport.tractor_plate/trailer_plate + plate_country_code → гр.21 (идентификация ТС)
- carrier → сведения о перевозчике → гр.44
- route_countries → маршрут/страны следования
- cmr_number/series/date + related_documents (Invoice, Packing List и т.п.) → гр.44
- transport.mode='road', mode_code_border=30, mode_code_inland=30 (гр.25/26)
- seals, volume_m3, special_instructions, driver, transshipment → доп. сведения (гр.31/44)

Если поле отсутствует в документе, НЕ выдумывайте — просто опустите необязательное.
Сохраняйте числа в килограммах и кубических метрах, если единицы явно указаны иначе — конвертируйте и укажите уже в целевых единицах.
"""