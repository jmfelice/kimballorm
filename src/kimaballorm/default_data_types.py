from sqlalchemy import Date, Integer, Numeric, String, Boolean

DEFAULT_VALUES = {
    Date: '1900-01-01',
    Integer: 0,
    Numeric: 0.0,
    String: '',
    Boolean: False
}
