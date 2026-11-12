"""Native Tableau XML presentation. All sources stay at their own analytical grain.

The XML conventions follow Tableau's installed sample workbooks. A Python/XML
check cannot replace opening the generated workbook in Tableau Desktop.
"""

from copy import deepcopy
import xml.etree.ElementTree as ET


WIDTH, HEIGHT = 1760, 1160
NAVY, TEAL, AMBER, MUTED = "#17324D", "#087F83", "#C37725", "#63758A"
SOURCE_TITLES = {"kpis": "Batch KPIs", "funnel": "Ordered funnel", "channels": "Channel comparison"}
REAL_FIELDS = {"account_rate", "transaction_rate", "from_start_rate", "dropoff_rate"}
INTEGER_FIELDS = {"started_sessions", "account_opened_sessions", "first_transaction_sessions",
                  "not_completed_sessions", "step", "sessions", "previous_sessions",
                  "dropoff_sessions", "quality_warnings"}


def child(parent, tag, text=None, **attributes):
    node = ET.SubElement(parent, tag, {key.replace("_", "-"): str(value) for key, value in attributes.items()})
    if text is not None:
        node.text = str(text)
    return node


def quoted(value):
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def style(parent, element, **attributes):
    rule = child(parent, "style-rule", element=element)
    for name, value in attributes.items():
        child(rule, "format", attr=name.replace("_", "-"), value=value)
    return rule


def formatted(parent, text, size=11, color=NAVY, bold=False):
    node = ET.Element("formatted-text")
    # Tableau places formatted content before a zone's style definition.
    parent.insert(0, node) if parent.tag == "zone" else parent.append(node)
    child(node, "run", text, fontname="Arial", fontsize=size, fontcolor=color, bold=str(bold).lower())
    return node


def parameter(labels, default):
    source = ET.Element("datasource", {"hasconnection": "false", "inline": "true", "name": "Parameters", "version": "18.1"})
    child(source, "aliases", enabled="yes")
    column = child(source, "column", caption="Select dataset", datatype="string", name="[Batch]",
                   param_domain_type="list", role="measure", type="nominal", value=quoted(default))
    child(column, "calculation", **{"class": "tableau", "formula": quoted(default)})
    members = child(column, "members")
    for label in labels:
        child(members, "member", value=quoted(label))
    return source


def datasource(name, rows):
    source = ET.Element("datasource", {"caption": SOURCE_TITLES[name], "inline": "true", "name": "textscan." + name, "version": "18.1"})
    connection = child(source, "connection", **{"class": "textscan", "directory": "Data", "filename": name + ".csv", "password": "", "server": ""})
    relation = child(connection, "relation", name=name + ".csv", table=f"[{name}#csv]", type="table")
    cols = child(relation, "columns", character_set="UTF-8", header="yes", locale="en_US", separator=",")
    fields = {}
    for ordinal, field in enumerate(rows[0]):
        datatype = "real" if field in REAL_FIELDS else "integer" if field in INTEGER_FIELDS else "string"
        child(cols, "column", datatype=datatype, name=field, ordinal=ordinal)
        fields[field] = datatype
    child(source, "aliases", enabled="yes")
    for field, datatype in fields.items():
        numeric = datatype != "string"
        column = child(source, "column", caption=field.replace("_", " ").title(), datatype=datatype,
                       name=f"[{field}]", role="measure" if numeric else "dimension", type="quantitative" if numeric else "nominal")
        if numeric:
            column.set("default-format", "p0.0%" if datatype == "real" else "n#,##0")
    selected = child(source, "column", caption="Selected dataset", datatype="boolean", name="[Selected batch]", role="dimension", type="nominal")
    child(selected, "calculation", **{"class": "tableau", "formula": "[dataset_label] = [Parameters].[Batch]"})
    if name == "funnel":
        col = child(source, "column", datatype="boolean", name="[Has previous stage]", role="dimension", type="nominal")
        child(col, "calculation", **{"class": "tableau", "formula": "[step] > 1"})
    return source


def field_instance(field, numeric=False):
    return f"[{'sum' if numeric else 'none'}:{field}:{'qk' if numeric else 'nk'}]"


def worksheet(parent, sources, param, name, source_name, *, row=None, measure=None,
              label=None, color=TEAL, parts=None, title=None, dropoff=False, text_size=12,
              row_width=200):
    sheet = child(parent, "worksheet", name=name)
    layout = child(sheet, "layout-options")
    formatted(child(layout, "title"), title or name, size=13, bold=True)
    table = child(sheet, "table")
    view = child(table, "view")
    source = sources[source_name]
    ds = source.get("name")
    refs = child(view, "datasources")
    child(refs, "datasource", caption=source.get("caption"), name=ds)
    child(refs, "datasource", name="Parameters")
    dep_param = child(view, "datasource-dependencies", datasource="Parameters")
    dep_param.append(deepcopy(param.find("column")))
    dep = child(view, "datasource-dependencies", datasource=ds)
    fields = {c.get("name")[1:-1]: c for c in source.findall("column")}
    for col in fields.values():
        dep.append(deepcopy(col))
    used = {"Selected batch"}
    for field in (row, measure, label):
        if field:
            used.add(field)
    for part in parts or []:
        if part.get("field"):
            used.add(part["field"])
    if dropoff:
        used.add("Has previous stage")
    references = {}
    for field in sorted(used):
        numeric = fields[field].get("datatype") in ("integer", "real")
        instance = field_instance(field, numeric)
        child(dep, "column-instance", column=f"[{field}]", derivation="Sum" if numeric else "None",
              name=instance, pivot="key", type="quantitative" if numeric else "nominal")
        references[field] = f"[{ds}].{instance}"
    filter_fields = ["Selected batch"] + (["Has previous stage"] if dropoff else [])
    for field in filter_fields:
        filter_node = child(view, "filter", **{"class": "categorical", "column": references[field]})
        child(filter_node, "groupfilter", function="member", level=field_instance(field), member="true")
    slices = child(view, "slices")
    for field in filter_fields:
        child(slices, "column", references[field])
    child(view, "aggregation", value="true")
    table_style = child(table, "style")
    style(table_style, "worksheet", display_field_labels="false", background_color="#FFFFFF")
    # Tableau's <format> vocabulary uses color/size. The fontcolor attribute
    # belongs to formatted-text <run> elements, not <format attr="...">.
    style(table_style, "cell", font_family="Arial", font_size=text_size, color=NAVY, text_align="left")
    header_style = style(table_style, "header", font_family="Arial", font_size=10, color=NAVY)
    if row:
        child(header_style, "format", attr="width", field=references[row], value=row_width)
    style(table_style, "label", font_family="Arial", font_size=10, color=NAVY)
    style(table_style, "pane", background_color="#FFFFFF")
    for element in ("gridline", "zeroline", "table-div"):
        style(table_style, element, line_visibility="off")
    if row:
        rule = child(table_style, "style-rule", element="worksheet")
        child(rule, "format", attr="display-field-labels", scope="rows", value="false")
    panes = child(table, "panes")
    pane = child(panes, "pane", selection_relaxation_option="selection-relaxation-disallow")
    child(child(pane, "view"), "breakdown", value="auto")
    child(pane, "mark", **{"class": "Bar" if measure else "Text"})
    encodings = child(pane, "encodings")
    text_fields = [label] if label else [p["field"] for p in (parts or []) if p.get("field")]
    for field in dict.fromkeys(text_fields):
        child(encodings, "text", column=references[field])
    if parts:
        custom = child(child(pane, "customized-label"), "formatted-text")
        for part in parts:
            value = f"<{references[part['field']]}>" if part.get("field") else part.get("text", "")
            # Tableau drops whitespace-only runs, so keep a line break with
            # the following field instead of emitting a separate "\n" run.
            value = part.get("prefix", "") + value
            child(custom, "run", value, fontname="Arial", fontsize=part.get("size", text_size),
                  fontcolor=part.get("color", NAVY), bold=str(part.get("bold", False)).lower())
    pane_style = child(pane, "style")
    style(pane_style, "mark", mark_color=color, mark_labels_show="true", mark_labels_cull="false", size="1")
    child(table, "rows", references[row] if row else "")
    child(table, "cols", references[measure] if measure else "")
    if not measure:
        child(table, "tooltip-style", tooltip_mode="none")
    return sheet


def zone(parent, identifier, x, y, w, h, *, padding=12, **attributes):
    result = child(parent, "zone", id=identifier, x=round(x / WIDTH * 100000), y=round(y / HEIGHT * 100000),
                   w=round(w / WIDTH * 100000), h=round(h / HEIGHT * 100000), **attributes)
    zs = child(result, "zone-style")
    for attr, value in (("border-style", "none"), ("border-width", "0"), ("margin", "0"),
                        ("padding", str(padding)), ("background-color", "#FFFFFF")):
        child(zs, "format", attr=attr, value=value)
    return result


def make_workbook(data, default_label):
    root = ET.Element("workbook", {"original-version": "18.1", "source-build": "2026.1.0", "source-platform": "win", "version": "18.1"})
    preferences = child(root, "preferences")
    child(preferences, "preference", name="ui.encoding.shelf.height", value="24")
    child(preferences, "preference", name="ui.shelf.height", value="26")
    sources_node = child(root, "datasources")
    param = parameter([r["dataset_label"] for r in data["kpis"]], default_label)
    sources_node.append(param)
    sources = {name: datasource(name, rows) for name, rows in data.items()}
    for source in sources.values():
        sources_node.append(source)
    sheets = child(root, "worksheets")
    def sheet(name, source="kpis", **options):
        return worksheet(sheets, sources, param, name, source, **options)
    sheet("Started sessions", parts=[
        {"text": "STARTED SESSIONS\n", "size": 10, "color": MUTED},
        {"field": "started_sessions", "size": 32, "bold": True},
        {"field": "started_detail", "prefix": "\n", "size": 10, "color": MUTED}])
    sheet("Account conversion", parts=[
        {"text": "ACCOUNT OPENING\n", "size": 10, "color": MUTED},
        {"field": "account_display", "size": 32, "bold": True, "color": TEAL},
        {"field": "account_detail", "prefix": "\n", "size": 10, "color": MUTED}])
    sheet("First transaction", parts=[
        {"text": "FIRST TRANSACTION\n", "size": 10, "color": MUTED},
        {"field": "transaction_display", "size": 32, "bold": True, "color": TEAL},
        {"field": "transaction_detail", "prefix": "\n", "size": 10, "color": MUTED}])
    sheet("Not completed", parts=[
        {"text": "NOT YET COMPLETED\n", "size": 10, "color": MUTED},
        {"field": "not_completed_sessions", "size": 32, "bold": True, "color": AMBER},
        {"text": "\nIn this saved snapshot", "size": 10, "color": MUTED}])
    sheet("Ordered onboarding funnel", "funnel", row="stage_display", measure="sessions", label="reach_label",
          color="#8BCDC3", title="01   Ordered onboarding funnel")
    sheet("Stage drop-off", "funnel", row="transition_display", measure="dropoff_sessions", label="loss_label",
          color="#E8B977", title="02   Sessions not progressing", dropoff=True, row_width=300)
    sheet("Channel conversion", "channels", row="channel", measure="transaction_rate", label="channel_label",
          color="#8CB4D3", title="03   First-transaction conversion by channel", row_width=120)
    sheet("Observed bottleneck", label="insight", text_size=12)
    sheet("Dataset context", label="context_label", text_size=10)
    sheet("Quality status", label="quality_label", text_size=10)
    dashboards = child(root, "dashboards")
    dashboard = child(dashboards, "dashboard", name="Onboarding Overview")
    formatted(child(child(dashboard, "layout-options"), "title"), "Digital banking | Onboarding performance", 22, bold=True)
    style(child(dashboard, "style"), "dashboard", background_color="#F1F5F8")
    child(dashboard, "size", maxheight=HEIGHT, maxwidth=WIDTH, minheight=HEIGHT, minwidth=WIDTH, sizing_mode="fixed")
    zones = child(dashboard, "zones")
    header = zone(zones, 1, 24, 8, 1040, 104, type_v2="text", padding=4)
    heading = formatted(header, "DIGITAL BANKING\n", size=10, color=MUTED, bold=True)
    child(heading, "run", "Onboarding performance", fontname="Arial", fontsize=25, fontcolor=NAVY, bold="true")
    selector = zone(zones, 2, 1160, 24, 576, 64, type_v2="paramctrl", param="[Parameters].[Batch]", mode="compact", custom_title="true", padding=4)
    formatted(selector, "Select dataset", size=10, color=MUTED)
    zone(zones, 3, 24, 116, 1712, 32, name="Dataset context", show_title="false", padding=4)
    for i, name in enumerate(("Started sessions", "Account conversion", "First transaction", "Not completed")):
        zone(zones, i + 4, 24 + i * 434, 164, 410, 160, name=name, show_title="false", padding=8)
    zone(zones, 8, 24, 344, 850, 410, name="Ordered onboarding funnel", show_title="true")
    zone(zones, 9, 898, 344, 838, 410, name="Stage drop-off", show_title="true")
    description = zone(zones, 10, 24, 760, 1712, 40, type_v2="text", padding=4)
    formatted(description, "Funnel labels: sessions | % of starts.   Drop-off labels: sessions | % of the previous stage.   KYC stage requires a pass.", size=9, color=MUTED)
    zone(zones, 11, 24, 810, 850, 254, name="Channel conversion", show_title="true")
    insight_title = zone(zones, 12, 898, 810, 838, 48, type_v2="text", padding=8)
    formatted(insight_title, "04   Observed bottleneck", size=13, bold=True)
    zone(zones, 13, 898, 858, 838, 206, name="Observed bottleneck", show_title="false")
    zone(zones, 14, 24, 1076, 1712, 31, name="Quality status", show_title="false", padding=4)
    note = zone(zones, 15, 24, 1106, 1712, 54, type_v2="text", padding=2)
    formatted(note, "SYNTHETIC DATA  |  Batch snapshot, not a live feed.  Incomplete does not imply permanent abandonment.\nChannel labels show conversion | transactions / starts. Differences are descriptive, not causal.", size=9, color=MUTED)
    windows = child(root, "windows")
    window = child(windows, "window", **{"class": "dashboard", "maximized": "true", "name": "Onboarding Overview"})
    viewpoints = child(window, "viewpoints")
    for s in sheets:
        child(child(viewpoints, "viewpoint", name=s.get("name")), "zoom", type="entire-view")
    child(window, "active", id="-1")
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)
