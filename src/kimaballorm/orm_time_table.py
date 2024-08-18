# from sqlalchemy import Column, UniqueConstraint
# from sqlalchemy import Date, DECIMAL, Integer, String, BIGINT
# from sqlalchemy.ext.declarative import declarative_base
#
# Base = declarative_base()
#
#
# class TimeTableStandard(Base):
#     __tablename__ = "bridge_time_table_standard"
#     __table_args__ = (
#         UniqueConstraint(
#             "period_ending",
#             "annum",
#             "duration"
#         ),
#         {"schema": "finance_dw"},
#     )
#     time_table_key = Column(Integer, primary_key = True, nullable = False, redshift_identity=(1, 1))
#     foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
#     period_ending = Column(Date, primary_key = True, nullable = False)
#     annum = Column(String(30), primary_key = True, nullable = False)
#     duration = Column(String(10), primary_key = True, nullable = False)
#     start_date = Column(Date, primary_key = True, nullable = False)
#     end_date = Column(Date, primary_key = True, nullable = False)
#     start_date_key = Column(Integer, primary_key = True, nullable = False)
#     end_date_key = Column(Integer, primary_key = True, nullable = False)
#     weighted_business_days = Column(DECIMAL(15, 3), primary_key = True, nullable = False)
#     __custom_info__ = ({"table_type": "Bridge"},)
#
#
#
