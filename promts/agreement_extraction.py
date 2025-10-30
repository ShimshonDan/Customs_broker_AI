from typing import Dict, Any

AGREEMENT_SCHEMA: Dict[str, Any] = {
  "type": "object",
  "additionalProperties": False,
  "properties": {
    "contract_number": { "type": "string", "description": "Номер договора → гр. 44" },
    "contract_date": {
      "type": "string",
      "description": "Дата договора (ISO 8601)",
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$"
    },

    "seller": {
      "type": "object",
      "additionalProperties": False,
      "properties": {
        "name": { "type": "string" },
        "legal_address": { "type": "string" },
        "country": { "type": "string" },
        "vat_or_reg_number": { "type": "string" },
        "bank_details": {
          "type": "object",
          "additionalProperties": False,
          "properties": {
            "beneficiary_name": { "type": "string" },
            "bank_name": { "type": "string" },
            "iban": { "type": "string" },
            "swift_bic": { "type": "string" },
            "account_number": { "type": "string" }
          }
        },
        "contacts": { "type": "string" }
      },
      "required": ["name"]
    },

    "buyer": {
      "type": "object",
      "additionalProperties": False,
      "properties": {
        "name": { "type": "string" },
        "legal_address": { "type": "string" },
        "country": { "type": "string" },
        "inn": { "type": "string" },
        "kpp": { "type": "string" },
        "ogrn": { "type": "string" },
        "bank_details": {
          "type": "object",
          "additionalProperties": False,
          "properties": {
            "beneficiary_name": { "type": "string" },
            "bank_name": { "type": "string" },
            "iban": { "type": "string" },
            "swift_bic": { "type": "string" },
            "account_number": { "type": "string" }
          }
        },
        "contacts": { "type": "string" }
      },
      "required": ["name"]
    },

    "subject": {
      "type": "string",
      "description": "Краткое описание товара/ассортимента (базовая формулировка для гр. 31)"
    },

    "incoterms": {
      "type": "object",
      "additionalProperties": False,
      "properties": {
        "rule": { "type": "string", "description": "Напр. FCA, DAP, EXW" },
        "place": { "type": "string", "description": "Место (город/склад/порт)" },
        "version": { "type": "string", "description": "Напр. Incoterms 2020" }
      },
      "required": ["rule", "place"]
    },

    "currency": {
      "type": "object",
      "additionalProperties": False,
      "properties": {
        "code": { "type": "string", "description": "ISO 4217, напр. EUR/USD/RUB", "pattern": "^[A-Z]{3}$" }
      },
      "required": ["code"]
    },

    "payment_terms": { "type": "string", "description": "Предоплата/отсрочка, сроки и т. п." },
    "transport_terms": {
      "type": "object",
      "additionalProperties": False,
      "properties": {
        "mode": { "type": "string", "description": "Вид транспорта, напр. road/air/sea/rail" },
        "details": { "type": "string" }
      }
    },

    "appendices": {
      "type": "array",
      "description": "Приложения/спецификации (№/дата) → ссылки в гр. 44",
      "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
          "type": { "type": "string", "description": "Напр. Specification, Appendix" },
          "number": { "type": "string" },
          "date": {
            "type": "string",
            "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$"
          }
        }
      }
    },

    "origin_and_manufacturer": {
      "type": "string",
      "description": "Если указано: страны происхождения/производители (для валидации гр. 34)"
    },

    "packaging_and_marking_requirements": {
      "type": "string",
      "description": "Требования к упаковке/маркировке (сверка с PL/CMR → гр. 31)"
    },

    "compliance_documents": {
      "type": "array",
      "description": "Сертификаты/CoC/Декларации, если перечислены → гр. 44 при необходимости",
      "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
          "type": { "type": "string" },
          "number": { "type": "string" },
          "date": {
            "type": "string",
            "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$"
          }
        }
      }
    },

    "catalog_references": {
      "type": "array",
      "description": "Артикулы/модели/каталожные номера (если перечислены)",
      "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
          "model_or_sku": { "type": "string" },
          "description": { "type": "string" }
        }
      }
    },

    "cross_links": {
      "type": "object",
      "additionalProperties": False,
      "description": "Для сверки с инвойсом/PL (если в тексте договора указаны ссылки)",
      "properties": {
        "invoice_ref": {
          "type": "object",
          "additionalProperties": False,
          "properties": {
            "number": { "type": "string" },
            "date": {
              "type": "string",
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$"
            }
          }
        },
        "packing_list_ref": {
          "type": "object",
          "additionalProperties": False,
          "properties": {
            "number": { "type": "string" },
            "date": {
              "type": "string",
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$"
            }
          }
        }
      }
    },

    "notes": { "type": "string" }
  },
  "required": [
    "contract_number",
    "contract_date",
    "seller",
    "buyer",
    "subject",
    "incoterms",
    "currency"
  ]
}

AGREEMENT_INSTRUCTION_RU = """Вы извлекаете данные из договора поставки (Contract) для подготовки ДТ и сквозной сверки с инвойсом/PL.
Верните СТРОГО валидный JSON по переданной JSON Schema. Никаких комментариев, текста или разметки — только JSON.

Маппинг на графы ДТ:
- contract_number + contract_date → гр. 44 (документ «Контракт»).
- seller → сверка с гр. 2; buyer → сверка с гр. 8 (наименования, юр. адреса, идентификаторы/VAT/ИНН/КПП).
- subject → базовая формулировка для гр. 31 (не противоречит инвойсу/PL).
- incoterms (rule+place[+version]) → гр. 20.
- currency.code (ISO 4217) → гр. 22 (базовая валюта сделки).
- payment_terms, transport_terms — справочно; сопоставьте с инвойсом и CMR (вид транспорта для гр. 25/26 косвенно).
- appendices/specifications, compliance_documents, cross_links → дополнительные документы в гр. 44 при необходимости.
- packaging_and_marking_requirements → сверка с PL/CMR (упаковка/Marks → гр. 31).
- origin_and_manufacturer (если указано) → валидация для гр. 34.

Правила:
1) НЕ выдумывайте отсутствующие сведения; необязательные поля опускайте.
2) Даты — в ISO 8601 (YYYY-MM-DD).
3) Валюту — в виде кода ISO 4217 (напр. EUR, USD, RUB).
4) Страны допустимо вернуть как полные названия или ISO-коды (если в документе явно указаны — сохраните, как есть).
5) Текст договора может ссылаться на приложения/спецификации/инвойс/PL — извлеките их как массивы с номером и датой.
6) Если встречаются артикулы/модели — перечислите их в catalog_references.
7) Возвращайте только валидный JSON согласно схеме, без лишних полей.
"""