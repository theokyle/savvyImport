# paths.py

# Contact
CONTACT_CSV = "./data/Contact.csv"

# Contact Cohort Association
CONTACT_COHORT_CSV = "./data/ContactCohortsAssociations.csv"

#Process
PROCESS_CSV = "./data/Deal.csv"
PROCESS_JOIN_PATHS = ["./data/ContactDealAssociations.csv", "./data/DealCohortsAssociations.csv"]

#Cohort
COHORT_CSV = "./data/Cohorts.csv"

#Company
COMPANY_CSV = "./data/Company.csv"
COMPANY_JOIN_PATHS = "./data/CompanyContactAssociations.csv"

# Engagement 
ENGAGEMENT_CALL_CSV = "./data/EngagementCall.csv"
ENGAGEMENT_MEETING_CSV = "./data/EngagementMeeting.csv"
ENGAGEMENT_EMAIL_CSV = "./data/EngagementEmail.csv"
ENGAGEMENT_NOTE_CSV = "./data/EngagementNote.csv"
ENGAGEMENT_TASK_CSV = "./data/EngagementTask.csv"

ENGAGEMENT_PATHS = [
    ENGAGEMENT_CALL_CSV,
    ENGAGEMENT_MEETING_CSV,
    ENGAGEMENT_EMAIL_CSV,
    ENGAGEMENT_NOTE_CSV,
    ENGAGEMENT_TASK_CSV
]

ENGAGEMENT_CONTACT_JOIN = "./data/EngagementContactAssociations.csv"
ENGAGEMENT_DEAL_JOIN = "./data/EngagementDealAssociations.csv"
ENGAGEMENT_COMPANY_JOIN = "./data/EngagementCompanyAssociations.csv"

ENGAGEMENT_JOIN_PATHS = {
    "contact": ENGAGEMENT_CONTACT_JOIN,
    "deal": ENGAGEMENT_DEAL_JOIN,
    "company": ENGAGEMENT_COMPANY_JOIN
}
