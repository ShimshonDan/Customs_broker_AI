import requests
from dotenv import load_dotenv
import base64
import jsonschema

import os
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

from promts import invoice_extraction as invoice
from promts import pl_extraction as package_list
from promts import CMR_extraction as cmr
from promts import agreement_extraction as agreement
from promts import DT_extraction as dt

load_dotenv()

PPLX_API_KEY = os.environ["PPLX_API_KEY"]
API_URL = "https://api.perplexity.ai/chat/completions"
MODEL = "sonar-pro"  

PROXY_USER = os.environ["PROXY_USER"]
PROXY_PASSWORD = os.environ["PROXY_PASSWORD"]
PROXY_HOST = os.environ["PROXY_HOST"]
PROXY_PORT = os.environ["PROXY_PORT"]

proxies = {
    "http":  f"socks5h://{PROXY_USER}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}",
    "https": f"socks5h://{PROXY_USER}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
}

OKEI = {
    "pcs": ("796", "шт"),
    "pc":  ("796", "шт"),
    "kg":  ("166", "кг"),
    "kilogram": ("166", "кг"),
}

def _build_hs_prompt_for_item(item: Dict[str, Any],
                              invoice_currency: Optional[str],
                              incoterms_str: Optional[str]) -> list:
    desc = item.get("description") or ""
    sku = item.get("model_or_sku") or ""
    qty = item.get("quantity")
    uom = item.get("uom") or ""
    net = item.get("net_weight") or item.get("net_weight_kg")
    gross = item.get("gross_weight") or item.get("gross_weight_kg")
    origin = item.get("origin_country") or ""
    manufacturer = item.get("manufacturer") or ""

    details_lines = [
        f"Описание: {desc}",
        f"Модель/артикул: {sku}" if sku else "",
        f"Количество/единица: {qty} {uom}".strip(),
        f"Вес нетто, кг: {net}" if net is not None else "",
        f"Вес брутто, кг: {gross}" if gross is not None else "",
        f"Страна происхождения: {origin}" if origin else "",
        f"Производитель: {manufacturer}" if manufacturer else "",
        f"Валюта инвойса: {invoice_currency}" if invoice_currency else "",
        f"Условия поставки: {incoterms_str}" if incoterms_str else "",
    ]
    details = "\n".join([x for x in details_lines if x])

    return [
        {"type": "text", "text": dt.DT_INSTRUCTION_RU},  # ИНСТРУКЦИЯ из DT_extraction
        {"type": "text", "text": "Данные позиции (используй для классификации и веб-поиска):\n" + details}
    ]

def classify_items_eaeu(invoice_json: Dict[str, Any], pl_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Индексируем PL, чтобы дополнить недостающие поля (масса, происхождение, упаковка)
    pl_index = {}
    for it in pl_json.get("items", []) or []:
        key = ((it.get("model_or_sku") or "").strip().lower(),
               (it.get("description") or "").strip().lower())
        pl_index[key] = it

    def enrich(inv_item: Dict[str, Any]) -> Dict[str, Any]:
        k = ((inv_item.get("model_or_sku") or "").strip().lower(),
             (inv_item.get("description") or "").strip().lower())
        extra = pl_index.get(k, {})
        merged = dict(inv_item)
        # берём поля из PL, которых нет в инвойсе
        for kk, vv in extra.items():
            if merged.get(kk) in (None, "", 0):
                merged[kk] = vv
        return merged

    currency = ((invoice_json.get("currency") or {}).get("code") or "").upper() or None
    inc = invoice_json.get("incoterms") or {}
    incoterms_str = (f"{inc.get('rule','')} {inc.get('place','')}".strip()
                     + (f", {inc.get('version')}" if inc.get('version') else "")) or None

    results: List[Dict[str, Any]] = []
    for idx, inv_item in enumerate(invoice_json.get("items") or [], start=1):
        merged_item = enrich(inv_item)
        message = _build_hs_prompt_for_item(merged_item, currency, incoterms_str)

        try:
            hs = _call_perplexity(message, dt.HS_SCHEMA, temperature=0.1, web_search=True)  # СХЕМА из DT_extraction
            # быстрая проверка
            code = hs.get("eaeu_hs_code")
            if not code or len(code) != 10 or not code.isdigit():
                raise ValueError(f"некорректный код: {code}")
            if len(hs.get("explanations", [])) != 5:
                raise ValueError("нужно ровно 5 строк объяснений.")
            results.append({
                "line_index": idx,
                "description": inv_item.get("description"),
                "model_or_sku": inv_item.get("model_or_sku"),
                "eaeu_hs_code": code,
                "confidence": hs.get("confidence"),
                "explanations": hs.get("explanations"),
                "candidate_codes": hs.get("candidate_codes", []),
                "evidence_urls": hs.get("evidence_urls", []),
                "notes": hs.get("notes", "")
            })
        except Exception as e:
            results.append({
                "line_index": idx,
                "description": inv_item.get("description"),
                "model_or_sku": inv_item.get("model_or_sku"),
                "error": f"HS-классификация не получена: {e}"
            })
    return results

def classify_items_eaeu(invoice_json: Dict[str, Any], pl_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Индексируем PL, чтобы дополнить недостающие поля (масса, происхождение, упаковка)
    pl_index = {}
    for it in pl_json.get("items", []) or []:
        key = ((it.get("model_or_sku") or "").strip().lower(),
               (it.get("description") or "").strip().lower())
        pl_index[key] = it

    def enrich(inv_item: Dict[str, Any]) -> Dict[str, Any]:
        k = ((inv_item.get("model_or_sku") or "").strip().lower(),
             (inv_item.get("description") or "").strip().lower())
        extra = pl_index.get(k, {})
        merged = dict(inv_item)
        # берём поля из PL, которых нет в инвойсе
        for kk, vv in extra.items():
            if merged.get(kk) in (None, "", 0):
                merged[kk] = vv
        return merged

    currency = ((invoice_json.get("currency") or {}).get("code") or "").upper() or None
    inc = invoice_json.get("incoterms") or {}
    incoterms_str = (f"{inc.get('rule','')} {inc.get('place','')}".strip()
                     + (f", {inc.get('version')}" if inc.get('version') else "")) or None

    results: List[Dict[str, Any]] = []
    for idx, inv_item in enumerate(invoice_json.get("items") or [], start=1):
        merged_item = enrich(inv_item)
        message = _build_hs_prompt_for_item(merged_item, currency, incoterms_str)

        try:
            hs = _call_perplexity(message, dt.HS_SCHEMA, temperature=0.1, web_search=True)  # СХЕМА из DT_extraction
            # быстрая проверка
            code = hs.get("eaeu_hs_code")
            if not code or len(code) != 10 or not code.isdigit():
                raise ValueError(f"некорректный код: {code}")
            if len(hs.get("explanations", [])) != 5:
                raise ValueError("нужно ровно 5 строк объяснений.")
            results.append({
                "line_index": idx,
                "description": inv_item.get("description"),
                "model_or_sku": inv_item.get("model_or_sku"),
                "eaeu_hs_code": code,
                "confidence": hs.get("confidence"),
                "explanations": hs.get("explanations"),
                "candidate_codes": hs.get("candidate_codes", []),
                "evidence_urls": hs.get("evidence_urls", []),
                "notes": hs.get("notes", "")
            })
        except Exception as e:
            results.append({
                "line_index": idx,
                "description": inv_item.get("description"),
                "model_or_sku": inv_item.get("model_or_sku"),
                "error": f"HS-классификация не получена: {e}"
            })
    return results

def _call_perplexity(message_content: list, schema, *, temperature: float = 0.2, web_search: bool = False) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {PPLX_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": message_content}],
        "response_format": {"type": "json_schema", "json_schema": {"schema": schema}},
        "temperature": temperature
    }
    if web_search:
        # ключевое: просим Perplexity сравнить с интернет-источниками
        payload["web_search_options"] = {"search": True, "search_type": "pro"}

    resp = requests.post(API_URL, headers=headers, proxies=proxies, data=json.dumps(payload), timeout=120)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    return json.loads(content)

def extract_from_pdf_file(path_to_pdf: str, instruction, schema) -> Dict[str, Any]:
    """
    Вариант 2: локальный PDF — шлем base64 (без data: префикса).
    """
    with open(path_to_pdf, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    message_content = [
        {"type": "text", "text": instruction},
        {"type": "file_url", "file_url": {"url": b64}, "file_name": os.path.basename(path_to_pdf)},
    ]
    return _call_perplexity(message_content, schema)

def validate_result(data: Dict[str, Any], schema) -> None:
    jsonschema.validate(instance=data, schema=schema)

def load_json(p: str | Path) -> Dict[str, Any]:
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def fmt_incoterms(src: Dict[str, Any]) -> Optional[str]:
    if not src:
        return None
    rule = src.get("rule")
    place = src.get("place")
    version = src.get("version")
    if not (rule and place):
        return None
    return f"{rule} {place}" + (f", {version}" if version else "")

def party_from_invoice_or_contract(invoice: Dict, contract: Dict, key: str) -> Dict[str, Any]:
    """
    key: 'seller' | 'buyer'
    Предпочтение инвойсу (там чаще финальные наимен./адреса), иначе договор.
    """
    a = (invoice.get(key) or {}) if invoice else {}
    b = (contract.get(key) or {}) if contract else {}
    return {**b, **a}  # invoice override

def normalize_country(c: Optional[str]) -> Optional[str]:
    if not c:
        return None
    return c.strip()

def money(v: Any, cur: Optional[str]) -> str:
    if v is None:
        return "—"
    try:
        val = float(v)
    except Exception:
        return str(v)
    return f"{val:,.2f}".replace(",", " ").replace(".00", ".00") + (f" {cur}" if cur else "")

def uom_okei(uom: Optional[str]) -> Tuple[str, str]:
    if not uom:
        return ("", "")
    key = uom.strip().lower()
    return OKEI.get(key, ("", uom))

# ——— сопоставление позиций invoice ↔ PL ———
def index_items(items: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Ключ = (model_or_sku.lower(), description.lower()) с fallback.
    """
    idx = {}
    for it in items or []:
        m = (it.get("model_or_sku") or "").strip().lower()
        d = (it.get("description") or "").strip().lower()
        key = (m or "", d or "")
        idx[key] = it
    return idx

def best_key(model_or_sku: Optional[str], description: Optional[str]) -> Tuple[str, str]:
    m = (model_or_sku or "").strip().lower()
    d = (description or "").strip().lower()
    return (m or "", d or "")

# ——— генерация текста для ДТ ———
def build_dt_text(invoice: Dict, pl: Dict, cmr: Dict, contract: Dict) -> str:
    # базовые источники
    currency_code = ((invoice.get("currency") or {}).get("code") or "").upper() or None
    total_amount = invoice.get("total_amount")

    # стороны: продавец/покупатель (инвойс > договор)
    seller = party_from_invoice_or_contract(invoice, contract, "seller")
    buyer  = party_from_invoice_or_contract(invoice, contract, "buyer")

    # страны отправления/назначения из CMR
    country_dispatch = None
    if cmr.get("place_and_date_taking_over", {}).get("place"):
        # попытаемся ISO, если уже ISO — ок; иначе оставим как есть
        country_dispatch = normalize_country(cmr.get("route_countries", [None])[0] or None) \
                           or normalize_country((cmr["place_and_date_taking_over"]["place"] or "").split(",")[-1])

    country_destination = None
    if cmr.get("place_of_delivery", {}).get("country"):
        country_destination = normalize_country(cmr["place_of_delivery"]["country"])
    else:
        # fallback: последний код маршрута
        rc = cmr.get("route_countries") or []
        country_destination = normalize_country(rc[-1] if rc else None)

    # Incoterms (инвойс > договор)
    incoterms_str = fmt_incoterms(invoice.get("incoterms") or {}) or fmt_incoterms(contract.get("incoterms") or {})

    # транспорт
    tr = cmr.get("transport") or {}
    tractor = tr.get("tractor_plate")
    trailer = tr.get("trailer_plate")
    plate_cc = tr.get("plate_country_code")

    # упаковка/марки PL
    pl_packages = pl.get("packages", {})
    pl_marks = (pl_packages or {}).get("marks_and_numbers") or ""

    # документы для гр. 44
    doc_44 = []
    if contract:
        doc_44.append(("Contract", contract.get("contract_number"), contract.get("contract_date")))
        for app in contract.get("appendices", []) or []:
            doc_44.append((app.get("type") or "Appendix", app.get("number"), app.get("date")))
        for link_name, ref in (contract.get("cross_links") or {}).items():
            if ref:
                doc_44.append((link_name.replace("_", " ").title(), ref.get("number"), ref.get("date")))
    if invoice:
        doc_44.append(("Invoice", invoice.get("invoice_number"), invoice.get("invoice_date")))
        if (invoice.get("contract_reference") or {}).get("number"):
            cr = invoice["contract_reference"]
            doc_44.append(("Contract (ref in invoice)", cr.get("number"), cr.get("date")))
    if pl:
        doc_44.append(("Packing List", pl.get("pl_number"), pl.get("pl_date")))
    if cmr:
        doc_44.append(("CMR", cmr.get("cmr_number"), cmr.get("cmr_date")))
        for r in cmr.get("related_documents") or []:
            doc_44.append((r.get("type"), r.get("number"), r.get("date")))

    # ——— шапка по графам ———
    header_lines = []
    header_lines.append(f"[2] Отправитель (продавец) — {seller.get('name') or '—'}; "
                        f"{seller.get('address') or seller.get('legal_address') or '—'}; "
                        f"VAT/рег: {seller.get('vat_or_reg_number') or '—'}")
    header_lines.append(f"[8] Получатель (покупатель/импортер) — {buyer.get('name') or '—'}; "
                        f"{buyer.get('address') or buyer.get('legal_address') or '—'}; "
                        f"ИНН: {buyer.get('inn') or '—'}; КПП: {buyer.get('kpp') or '—'}")
    header_lines.append(f"[15] Страна отправления — {country_dispatch or '—'}")
    header_lines.append(f"[17] Страна назначения — {country_destination or '—'}")
    header_lines.append(f"[20] Условия поставки — {incoterms_str or '—'}")
    header_lines.append(f"[21] Идентификация ТС — тягач: {tractor or '—'}; прицеп: {trailer or '—'}; "
                        f"страна номера: {plate_cc or '—'}")
    header_lines.append(f"[22] Валюта и сумма по счёту — {currency_code or '—'}; Итого: {money(total_amount, currency_code)}")
    header_lines.append(f"[25] Вид транспорта на границе — 30 (автодорожный)")
    header_lines.append(f"[26] Вид транспорта внутри страны — 30 (автодорожный)")
    # 31 (общий): краткое резюме упаковки/маркировок
    header_lines.append(f"[31] Упаковка/маркировка (сводно) — {pl.get('packages', {}).get('total_packages') or '—'} мест; "
                        f"{pl.get('packages', {}).get('package_type') or '—'}; Marks: {pl_marks or '—'}")
    header_lines.append(f"[33] Код товара (ТН ВЭД ЕАЭС) — требуется классификация")
    # 34 (общий): может отсутствовать — заполняется по позициям/сертификатам, оставим примечание
    header_lines.append(f"[34] Страна происхождения — по позициям/документам происхождения (если указаны)")
    # 35/38 общие — лучше из PL + сверка с CMR
    g_total = pl.get("gross_weight_total") or pl.get("gross_weight_total_kg") or cmr.get("gross_weight_total_kg")
    n_total = pl.get("net_weight_total") or pl.get("net_weight_total_kg") or cmr.get("net_weight_total_kg")
    header_lines.append(f"[35] Вес брутто (общий) — {g_total if g_total is not None else '—'} кг")
    header_lines.append(f"[38] Вес нетто (общий) — {n_total if n_total is not None else '—'} кг")
    header_lines.append(f"[41] Доп. ед. изм./количество — см. по позициям; привести к кодам ОКЕИ/ЕАЭС")
    header_lines.append(f"[42] Цена за единицу — см. по позициям (из инвойса)")
    # 44 документы
    if doc_44:
        doc_lines = "; ".join([f"{t or 'Док.'} №{(n or '—')} от {(d or '—')}" for (t, n, d) in doc_44])
    else:
        doc_lines = "—"
    header_lines.append(f"[44] Доп. документы — {doc_lines}")
    header_lines.append(f"[46] Статистическая стоимость — расчётная (по правилам статистики/курсам)")
    header_lines.append(f"[47] Налоги/платежи — расчётные (ставки по коду ТН ВЭД)")

    # ——— построчно по позициям ———
    # индексы
    inv_items = invoice.get("items") or []
    pl_items  = pl.get("items") or []
    idx_pl = index_items(pl_items)

    lines_block = []
    for i, inv in enumerate(inv_items, start=1):
        key = best_key(inv.get("model_or_sku"), inv.get("description"))
        pli = idx_pl.get(key)
        # если не нашли по ключу, попробуем тупо по description
        if not pli and key[1]:
            pli = next((x for x in pl_items if (x.get("description") or "").strip().lower() == key[1]), None)

        desc = inv.get("description") or (pli or {}).get("description") or "—"
        model = inv.get("model_or_sku") or (pli or {}).get("model_or_sku") or ""
        if model and model.lower() not in desc.lower():
            desc_show = f"{desc}; модель/артикул: {model}"
        else:
            desc_show = desc

        qty = inv.get("quantity")
        uom = inv.get("uom") or (pli or {}).get("uom")
        okei_code, okei_name = uom_okei(uom)
        gross = (pli or {}).get("gross_weight") or (pli or {}).get("gross_weight_kg")
        net   = (pli or {}).get("net_weight") or (pli or {}).get("net_weight_kg")
        packaging = (pli or {}).get("packaging") or {}
        packs_str = []
        if packaging:
            if packaging.get("packages_qty"): packs_str.append(f"{packaging['packages_qty']} мест")
            if packaging.get("package_type"): packs_str.append(str(packaging['package_type']))
            if packaging.get("marks_range"): packs_str.append(f"Marks: {packaging['marks_range']}")
        packs_show = ", ".join(packs_str) or "—"

        origin = (inv.get("origin_country")
                  or (pli or {}).get("origin_country")
                  or "—")
        unit_price = inv.get("unit_price")
        line_total = inv.get("line_total")

        lines_block.append(f"[31] Позиция {i}: {desc_show}. Упаковка/маркировка: {packs_show}")
        lines_block.append(f"[34] Страна происхождения — {origin}")
        lines_block.append(f"[35] Вес брутто (кг) — {gross if gross is not None else '—'}")
        lines_block.append(f"[38] Вес нетто (кг) — {net if net is not None else '—'}")
        if qty is not None or uom:
            lines_block.append(f"[41] Кол-во/ед. — {qty if qty is not None else '—'} {uom or ''}"
                               + (f" (ОКЕИ {okei_code} {okei_name})" if okei_code else ""))
        lines_block.append(f"[42] Цена за единицу — {money(unit_price, currency_code)}")
        lines_block.append(f"[45] Таможенная стоимость по строке — требуется расчёт (учёт Incoterms/фрахта/страховки)")
        lines_block.append(f"[46] Стат. стоимость по строке — расчёт от таможенной стоимости (валюта статистики)")
        
        lines_block.append("")

    text = "\n".join(header_lines) + "\n\n" + "\n".join(lines_block).rstrip()
    return text

if __name__ == "__main__":
    invoice_ = extract_from_pdf_file("./docs/invoice_GM-INV-2025-384.pdf", 
                                 invoice.INVOICE_INSTRUCTION_RU, 
                                 invoice.INVOICE_SCHEMA)
    
    validate_result(invoice_, schema=invoice.INVOICE_SCHEMA)
    json.dump(invoice_, open("docs_json/invoice.json", "w"), indent=True)
    
    pl = extract_from_pdf_file("./docs/packing_list_PL-2025-384.pdf", 
                                 package_list.PL_INSTRUCTION_RU,
                                 package_list.PACKING_LIST_SCHEMA)
    
    validate_result(pl, schema=package_list.PACKING_LIST_SCHEMA)
    json.dump(pl, open("docs_json/pl.json", "w"), indent=True)

    cmr_ = extract_from_pdf_file("./docs/CMR.pdf", 
                                 cmr.CMR_INSTRUCTION_RU,
                                 cmr.CMR_SCHEMA)
    
    validate_result(cmr_, schema=cmr.CMR_SCHEMA)
    json.dump(cmr_, open("docs_json/CMR.json", "w"), indent=True)

    contract = extract_from_pdf_file("./docs/ved-dogovor_TI-GM-2025-012.pdf", 
                                 agreement.AGREEMENT_INSTRUCTION_RU,
                                 agreement.AGREEMENT_SCHEMA)
    
    validate_result(contract, schema=agreement.AGREEMENT_SCHEMA)
    json.dump(contract, open("docs_json/agreement.json", "w"), indent=True)

    dt_text = build_dt_text(invoice_, pl, cmr_, contract)
    hs_results = classify_items_eaeu(invoice_, pl)

    lines = []
    for r in hs_results:
        if "error" in r:
            lines.append(f"[33] Позиция {r['line_index']}: ошибка — {r['error']}\n")
            continue
        lines.append(f"[33] Позиция {r['line_index']}: код ТН ВЭД ЕАЭС {r['eaeu_hs_code']} (доверие {r.get('confidence')})")
        for s in r.get("explanations") or []:   
            lines.append(f"  - {s}")
        if r.get("candidate_codes"):
            lines.append("  Альтернативы:")
            for c in r["candidate_codes"]:
                lines.append(f"    • {c['code']}: {c['why_not']}")
        if r.get("evidence_urls"):
            lines.append("  Источники:")
            for url in r["evidence_urls"]:
                lines.append(f"    - {url}")
        lines.append("")

    hs_text = "\n".join(lines).rstrip()
    full_text = dt_text + "\n\n" + hs_text
    print(full_text)

    Path("out").mkdir(exist_ok=True)
    with open("out/hs_classification.txt", "w", encoding="utf-8") as f:
        f.write(hs_text)
    with open("out/dt_mapping_with_hs.txt", "w", encoding="utf-8") as f:
        f.write(full_text)