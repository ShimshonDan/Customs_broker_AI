from typing import Dict, Any

PACKING_LIST_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        # ===== HEADER (по отправке) =====
        "pl_number": {"type": "string", "description": "Номер PL → гр. 44 (ссылка на документ)"},
        "pl_date": {
            "type": "string",
            "description": "Дата PL (ISO 8601 предпочтительно)",
            "pattern": r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"
        },
        "invoice_ref": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "number": {"type": "string", "description": "Связь с инвойсом → гр. 44"},
                "date": {
                    "type": "string",
                    "pattern": r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"
                }
            }
        },
        "packages": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "total_packages": {"type": "number", "description": "Всего мест"},
                "package_type": {"type": "string", "description": "Тип тары (коробки/паллеты/ящики и т.п.)"},
                "marks_and_numbers": {"type": "string", "description": "Маркировка/номера мест → гр. 31"},
            }
        },
        "gross_weight_total": {"type": "number", "description": "Общий вес брутто → гр. 35"},
        "net_weight_total": {"type": "number", "description": "Общий вес нетто → гр. 38"},
        "dimensions_total": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "length_cm": {"type": "number"},
                "width_cm": {"type": "number"},
                "height_cm": {"type": "number"},
                "volume_m3": {"type": "number"}
            }
        },

        # ===== ITEMS (по позициям) =====
        "items": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "line_no": {"type": "string"},
                    "description": {"type": "string", "description": "Наименование/описание → гр. 31"},
                    "model_or_sku": {"type": "string", "description": "Артикул/модель (связка с инвойсом/ТН ВЭД)"},
                    "quantity": {"type": "number", "description": "Кол-во → гр. 41 (доп. ед.)"},
                    "uom": {"type": "string", "description": "Ед. изм. (шт/кг/компл и т.п.)"},
                    "net_weight": {"type": "number", "description": "Нетто по позиции → гр. 38 (строка)"},
                    "gross_weight": {"type": "number", "description": "Брутто по позиции → гр. 35 (строка)"},
                    "packaging": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "packages_qty": {"type": "number", "description": "Кол-во мест по позиции"},
                            "package_type": {"type": "string", "description": "Тип тары по позиции"},
                            "marks_range": {"type": "string", "description": "Диапазон/список маркировок, напр. 'Boxes #1–8'"},
                        }
                    },
                    "origin_country": {"type": "string", "description": "Страна происхождения → гр. 34 (если указано)"},
                    "manufacturer": {"type": "string"},
                    "dimensions": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "length_cm": {"type": "number"},
                            "width_cm": {"type": "number"},
                            "height_cm": {"type": "number"},
                            "volume_m3": {"type": "number"}
                        }
                    }
                },
                "required": ["description", "quantity", "uom"]
            }
        },

        # Служебные заметки / OCR наблюдения
        "notes": {"type": "string"}
    },
    "required": ["pl_number", "pl_date", "items"]
}

# — Подсказка модели —
PL_INSTRUCTION_RU = """Вы извлекаете данные из упаковочного листа (Packing List, PL) для подготовки ДТ.
Верните СТРОГО валидный JSON по переданной JSON Schema. Никаких комментариев/текста, только JSON.

Маппинг на графы ДТ:
- pl_number / pl_date → графа 44 (документ PL)
- invoice_ref.number / invoice_ref.date → графа 44 (связь с инвойсом)
- packages.total_packages + package_type → графа 31 (упаковка)
- packages.marks_and_numbers → графа 31 (Marks & Numbers)
- gross_weight_total → графа 35 (общий брутто)
- net_weight_total → графа 38 (общий нетто)
- dimensions_total → доп. сведения гр. 31 (если есть)
По каждой позиции (items[]):
- description → гр. 31; model_or_sku → идентификация/связь с инвойсом/ТН ВЭД
- quantity + uom → гр. 41 (доп. ед.) и расчеты 42/46 совместно с инвойсом
- net_weight → гр. 38 (строка), gross_weight → гр. 35 (строка)
- packaging (packages_qty, package_type, marks_range) → гр. 31 (упаковка/marks)
- origin_country / manufacturer → гр. 34 (если указано)
- dimensions → доп. сведения гр. 31 (если указано)

Если поля нет в PL — не выдумывайте, просто опустите необязательное.
"""