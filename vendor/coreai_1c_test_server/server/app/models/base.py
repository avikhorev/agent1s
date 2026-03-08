"""Helpers to construct entity field lists for 1C OData entities."""


def _f(name, edm_type, **kw):
    d = {"name": name, "edm_type": edm_type}
    d.update(kw)
    return d


def catalog_fields(hierarchical=False, extra=None):
    fields = [
        _f("Ref_Key", "Edm.Guid", primary_key=True),
        _f("DeletionMark", "Edm.Boolean"),
        _f("Description", "Edm.String", max_length=200),
        _f("Code", "Edm.String", max_length=20),
    ]
    if hierarchical:
        fields += [
            _f("IsFolder", "Edm.Boolean"),
            _f("Parent_Key", "Edm.Guid", nullable=True),
        ]
    if extra:
        fields.extend(extra)
    return fields


def document_fields(extra=None):
    fields = [
        _f("Ref_Key", "Edm.Guid", primary_key=True),
        _f("DeletionMark", "Edm.Boolean"),
        _f("Number", "Edm.String", max_length=20),
        _f("Date", "Edm.DateTime"),
        _f("Posted", "Edm.Boolean"),
    ]
    if extra:
        fields.extend(extra)
    return fields


def tabular_fields(extra=None):
    fields = [
        _f("Ref_Key", "Edm.Guid", primary_key=True),
        _f("LineNumber", "Edm.Int32", primary_key=True),
    ]
    if extra:
        fields.extend(extra)
    return fields


def accumulation_register_fields(extra=None):
    fields = [
        _f("Ref_Key", "Edm.Guid", primary_key=True),
        _f("Period", "Edm.DateTime"),
        _f("Recorder_Key", "Edm.Guid"),
        _f("LineNumber", "Edm.Int32"),
        _f("Active", "Edm.Boolean"),
    ]
    if extra:
        fields.extend(extra)
    return fields


def information_register_fields(extra=None):
    fields = [
        _f("Ref_Key", "Edm.Guid", primary_key=True),
        _f("Period", "Edm.DateTime"),
    ]
    if extra:
        fields.extend(extra)
    return fields
