from typing import Dict, Any

INVOICE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "invoice_number": {"type": "string", "minLength": 1, "description": "Номер инвойса → гр. 44"},
        "invoice_date": {
            "type": "string",
            "description": "Дата инвойса (ISO 8601 предпочтительно)",
            "pattern": r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"
        },
        "seller": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "address": {"type": "string"},
                "country": {"type": "string"},
                "vat_or_reg_number": {"type": "string"}
            },
            "required": ["name"]
        },
        "buyer": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "inn": {"type": "string"},
                "kpp": {"type": "string"},
                "address": {"type": "string"}
            },
            "required": ["name"]
        },
        "incoterms": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "rule": {"type": "string"},
                "place": {"type": "string"},
                "version": {"type": "string"}
            },
            "required": ["rule", "place"]
        },
        "currency": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "code": {"type": "string", "pattern": "^[A-Z]{3}$"}
            },
            "required": ["code"]
        },
        "total_amount": {"type": "number"},
        "items": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "description": {"type": "string", "description": "гр. 31"},
                    "model_or_sku": {"type": "string", "description": "артикул/модель для маппинга на ТН ВЭД"},
                    "quantity": {"type": "number", "description": "гр. 41 количество"},
                    "uom": {"type": "string", "description": "ед. изм., гр. 41"},
                    "unit_price": {"type": "number", "description": "цена за единицу"},
                    "line_total": {"type": "number", "description": "сумма позиции"},
                    "origin_country": {"type": "string", "description": "гр. 34 (если указано)"},
                    "manufacturer": {"type": "string"}
                },
                "required": ["description", "quantity", "uom", "unit_price", "line_total"]
            }
        },
        "subtotal_ex_vat": {"type": "number"},
        "vat_amount": {"type": "number"},
        "grand_total": {"type": "number"},
        "charges": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "freight": {"type": "number"},
                "insurance": {"type": "number"},
                "packing": {"type": "number"},
                "discount": {"type": "number"}
            }
        },
        "contract_reference": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "number": {"type": "string"},
                "date": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"}
            }
        },
        "payment_terms": {"type": "string"},
        "bank_details": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "beneficiary_name": {"type": "string"},
                "bank_name": {"type": "string"},
                "iban": {"type": "string"},
                "swift_bic": {"type": "string"},
                "account_number": {"type": "string"}
            }
        },
        "notes": {"type": "string"}
    },
    "required": [
        "invoice_number", "invoice_date", "seller", "buyer",
        "incoterms", "currency", "total_amount", "items"
    ]
}

INVOICE_INSTRUCTION_RU = """Вы — извлекатель структурированных данных для таможенной декларации (ДТ).
У вас есть PDF инвойса. Верните СТРОГО JSON валидный под переданную JSON Schema.
Ничего, кроме JSON, не добавляйте (без комментариев/префиксов/markdown).

Требуемые поля (соответствие граф ДТ):
- invoice_number и invoice_date → графа 44 (сведения о документе)
- seller (гр. 2): name, address/country, vat_or_reg_number
- buyer (гр. 8): name, inn/kpp (если присутствуют), address
- incoterms (rule+place[+version]) → гр. 20 (напр. "FCA Berlin, Incoterms 2020")
- currency.code (ISO 4217) → гр. 22
- total_amount (итог к оплате) → гр. 22
- items[]:
  - description → гр. 31
  - model_or_sku → для маппинга на ТН ВЭД
  - quantity + uom → гр. 41
  - unit_price и line_total → для расчёта стоимости (основание гр. 42/45/46)
Дополнительно желательно извлечь: subtotal_ex_vat, vat_amount, grand_total; origin_country и manufacturer по позициям → гр. 34; charges.freight/insurance/packing/discount; contract_reference (number/date) → гр. 44; payment_terms и bank_details.

Если какого-то поля нет в документе — НЕ выдумывайте. Просто опустите необязательное поле.
"""
