from bson import ObjectId

# DEALSTAGE_TO_LABEL = {
#     # Pipeline 81041244
#     "163394135": "Wait List",
#     "152925963": "Closed won",
#     "152925964": "Closed lost",
#     "152872315": "Presenting for Admission",
#     "152925958": "Applied",
#     "152872316": "Contract Sent",
#     "152925961": "Candidate Meet & Greet",
#     "152925962": "Funding Approved",

#     # Pipeline 95297049
#     "174822194": "Placement Efforts Began",
#     "174762526": "Interviewing",
#     "174762525": "Actively Engaging",
#     "174764437": "Not Engaging",
#     "174822200": "Closed lost",
#     "174822199": "Closed won",
# }

OWNER_ID_TO_CONTACT_ID = {
    "84379854": ObjectId("693c2d632ad755420b481458"),  # Kyle Butler
    "554259518": ObjectId("655bd3f1278a3f80408a1605"),  # Matt Thomas
    "737334862": ObjectId("665a43ea82fd0900446b2521"),  # Stephanie Grimshaw
    "737334863": ObjectId("693a009c9ca1c01d0fe11789"),  # Justin Treadwell
    "739743375": ObjectId("693c21042ad755420b481432"),  # Laurie Wilson
}

DEALSTAGE_TO_STAGE = {
    "163394135": {"value": "Waitlist", "label": "Waitlist", "sortOrder": 2, "reasonRequired": False},
    "152925963": {"value": "Closed won", "label": "Closed won", "sortOrder": 6, "reasonRequired": False},
    "152925964": {"value": "Closed lost", "label": "Closed lost", "sortOrder": 7, "reasonRequired": True},
    "152872315": {"value": "Presenting for Admission", "label": "Presenting for Admission", "sortOrder": 4, "reasonRequired": False},
    "152925958": {"value": "Applied", "label": "Applied", "sortOrder": 0, "reasonRequired": False},
    "152872316": {"value": "Contract Sent", "label": "Contract Sent", "sortOrder": 5, "reasonRequired": False},
    "152925961": {"value": "Candidate Meet & Greet", "label": "Candidate Meet & Greet", "sortOrder": 1, "reasonRequired": False},
    "152925962": {"value": "Funding Approved", "label": "Funding Approved", "sortOrder": 3, "reasonRequired": False},
    "174822194": {"value": "Placement Efforts Began", "label": "Placement Efforts Began", "sortOrder": 0, "reasonRequired": False},
    "174762526": {"value": "Interviewing", "label": "Interviewing", "sortOrder": 2, "reasonRequired": False},
    "174762525": {"value": "Actively Engaging", "label": "Actively Engaging", "sortOrder": 1, "reasonRequired": False},
    "174764437": {"value": "Not Engaging", "label": "Not Engaging", "sortOrder": 3, "reasonRequired": False},
    "174822200": {"value": "Closed lost", "label": "Closed lost", "sortOrder": 5, "reasonRequired": True},
    "174822199": {"value": "Closed won", "label": "Closed won", "sortOrder": 4, "reasonRequired": False},
}

TRACTION_LEVELS = [ "Red", "Green", "Yellow"]

OWNER_TO_EMAIL = {
    "84379854": "theokyle@gmail.com",
    "93716953": "kayla@socialsutter.com",
    "554259518": "matsinet@gmail.com",
    "613913678": "elaine@savvycoders.com",
    "737334862": "stephanie@savvycoders.com",
    "737334863": "justin@savvycoders.com",
    "739743375": "wilson@savvycoders.com",
    "1483100634": "mrsasturm@gmail.com",
}
