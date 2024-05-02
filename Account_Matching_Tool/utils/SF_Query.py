view_query = """
set transaction isolation level read uncommitted

SELECT * FROM Salesforce.vwAccountMatchingData_TEST
ORDER BY AccountStatusScore asc, ParentScore asc
"""
