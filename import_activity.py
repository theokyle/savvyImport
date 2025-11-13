import os
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from dateutil import parser as dateparser
from normalize import parse_date


def import_activity(type, path, join_path, limit=None, dry_run=False):

    print(f"📥 Loading activities from: {path}")
    print(f"🔗 Joining with contact data from: {join_path}")

    df_activities = pd.read_csv(path, dtype=str).fillna("")
    df_join = pd.read_csv(join_path, dtype=str).fillna("")
    merged = pd.merge(df_activities, df_join, on="EngagementId", how="left")

    if limit:
        merged = merged.head(limit)

    print(f"✅ Merged {len(merged)} records")

    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    contacts_collection = db["Contact"]
    activities_collection = db["Activity"]

    operations = []
    skipped_rows = 0

    contacts = {
        str(c["externalId"]): c["_id"]
        for c in contacts_collection.find({}, {"externalId": 1})
    }

    for _, row in merged.iterrows():
        contact_vid = str(row.get("VId", "")).strip()
        if not contact_vid:
            print("⚠️ No contact vid in join record, skipping.")
            skipped_rows += 1
            continue

        contact_id = contacts.get(contact_vid)
        if not contact_id:
            print(f"⚠️ Contact not found for vid {contact_vid}, skipping.")
            skipped_rows += 1
            continue

        engagement_type = row['engagement_type'].strip().lower()
        match engagement_type:
            case "call":
                activity_doc = {
                    "type": row.get("hs_activity_type", "Call").strip(),
                    "contact": contact_id,
                    "author": row.get("hs_created_by_user_id", None),
                    "subject": row.get("hs_call_title", row.get("hs_call_summary", "")).strip(),
                    "content": row.get("hs_call_body", "").strip(),
                    "status": row.get("hs_call_status", "Completed").strip(),
                    "dueDate": parse_date(row.get("hs_createdate")),
                    "externalId": row.get("EngagementId"),
                    "source": "HubSpot",
                    "metadata": {
                        "duration": row.get("hs_call_duration"),
                        "direction": row.get("hs_call_direction"),
                        "disposition": row.get("hs_call_disposition"),
                        "recordingUrl": row.get("hs_call_recording_url"),
                        "fromNumber": row.get("hs_call_from_number"),
                        "toNumber": row.get("hs_call_to_number"),
                        "location": row.get("hs_call_location"),
                        "externalId": row.get("hs_call_unique_external_id"),
                        "createdBy": row.get("hs_created_by"),
                        "lastModified": parse_date(row.get("hs_lastmodifieddate")),
                    }
                }
            case "meeting":
                activity_doc = {
                    "type": row.get("hs_activity_type", "MEETING").strip(),   
                    "contact": contact_id,                                 
                    "author": row.get("hs_created_by_user_id", None),          
                    "subject": row.get("hs_meeting_title", "").strip(),       
                    "content": row.get("hs_meeting_body", "").strip(),         
                    "status": row.get("hs_meeting_outcome", "Scheduled").strip(), 
                    "dueDate": parse_date(row.get("hs_meeting_start_time")),  
                    "endDate": parse_date(row.get("hs_meeting_end_time")),  
                    "externalId": row.get("EngagementId"),   
                    "source": "HubSpot",
                    "metadata": {
                        "engagementId": row.get("EngagementId"),
                        "vid": row.get("vid"),
                        "objectId": row.get("hs_object_id"),
                        "iCalUid": row.get("hs_i_cal_uid"),
                        "guests": row.get("hs_guest_emails"),
                        "videoUrl": row.get("hs_video_conference_url"),
                        "videoPlatform": row.get("hs_video_conference_platform"),
                        "location": row.get("hs_meeting_location"),
                        "locationType": row.get("hs_meeting_location_type"),
                        "sourceId": row.get("hs_meeting_source_id"),
                        "source": row.get("hs_meeting_source"),
                        "createdBy": row.get("hs_created_by_user_id"),
                        "lastModified": parse_date(row.get("hs_lastmodifieddate")),
                        "internalNotes": row.get("hs_internal_meeting_notes"),
                        "bookingInstanceId": row.get("hs_booking_instance_id"),
                        "campaignGuid": row.get("hs_campaign_guid"),
                        "followUpAction": row.get("hs_follow_up_action"),
                        "followUpContext": row.get("hs_follow_up_context"),
                        "scheduledTasks": row.get("hs_scheduled_tasks"),
                        "attendanceDuration": row.get("hs_notetaker_attendance_duration"),
                        "timezone": row.get("hs_timezone"),
                        "allOwnerIds": row.get("hs_all_owner_ids"),
                        "allTeamIds": row.get("hs_all_team_ids"),
                        "assignedBusinessUnits": row.get("hs_all_assigned_business_unit_ids"),
                        "accessibleTeamIds": row.get("hs_all_accessible_team_ids"),
                    }
                }
            case "email":
                activity_doc = {
                    "type": row.get("hs_activity_type", "EMAIL").strip(),          
                    "contact": contact_id,                                      
                    "author": row.get("hs_created_by_user_id", None),               
                    "subject": row.get("hs_email_subject", "").strip(),            
                    "content": row.get("hs_body_preview", "").strip(),            
                    "status": row.get("hs_email_status", "Sent").strip(),           
                    "dueDate": parse_date(row.get("hs_createdate")),  
                    "externalId": row.get("EngagementId"),              
                    "source": "HubSpot",
                    "metadata": {
                        "vid": row.get("vid"),
                        "objectId": row.get("hs_object_id"),
                        "direction": row.get("hs_email_direction"),
                        "fromEmail": row.get("hs_email_from_email"),
                        "fromName": f"{row.get('hs_email_from_firstname', '')} {row.get('hs_email_from_lastname', '')}".strip(),
                        "toEmails": row.get("hs_email_to_email"),
                        "toNames": f"{row.get('hs_email_to_firstname', '')} {row.get('hs_email_to_lastname', '')}".strip(),
                        "ccEmails": row.get("hs_email_cc_email"),
                        "bccEmails": row.get("hs_email_bcc_email"),
                        "openCount": row.get("hs_email_open_count"),
                        "clickCount": row.get("hs_email_click_count"),
                        "replyCount": row.get("hs_email_reply_count"),
                        "threadId": row.get("hs_email_thread_id"),
                        "threadSummary": row.get("hs_email_thread_summary"),
                        "sendEventId": row.get("hs_email_send_event_id"),
                        "postSendStatus": row.get("hs_email_post_send_status"),
                        "mediaProcessingStatus": row.get("hs_email_media_processing_status"),
                        "messageId": row.get("hs_email_message_id"),
                        "engagementSource": row.get("hs_engagement_source"),
                        "lastModified": parse_date(row.get("hs_lastmodifieddate")),
                        "createdBy": row.get("hs_created_by_user_id"),
                        "sourceId": row.get("hs_engagement_source_id"),
                        "externalId": row.get("hs_object_id"),
                        "allOwnerIds": row.get("hs_all_owner_ids"),
                        "allTeamIds": row.get("hs_all_team_ids"),
                        "accessibleTeamIds": row.get("hs_all_accessible_team_ids"),
                        "assignedBusinessUnits": row.get("hs_all_assigned_business_unit_ids"),
                        "internalNotes": row.get("hs_email_details"),
                        "readOnly": row.get("hs_read_only"),
                    }
                }
            case "note":
                activity_doc = {
                    "type": row.get("hs_activity_type", "NOTE").strip(),  
                    "contact": contact_id,                             
                    "author": row.get("hs_created_by_user_id", None),       
                    "subject": None,                                        
                    "content": row.get("hs_note_body", "").strip(),         
                    "status": "Completed",                                  
                    "dueDate": parse_date(row.get("hs_createdate")),  
                    "externalId": row.get("EngagementId"),      
                    "source": "HubSpot",
                    "metadata": {
                        "vid": row.get("vid"),
                        "objectId": row.get("hs_object_id"),
                        "internalNotes": row.get("hs_note_body"),
                        "allOwnerIds": row.get("hs_all_owner_ids"),
                        "allTeamIds": row.get("hs_all_team_ids"),
                        "accessibleTeamIds": row.get("hs_all_accessible_team_ids"),
                        "assignedBusinessUnits": row.get("hs_all_assigned_business_unit_ids"),
                        "attachments": row.get("hs_attachment_ids"),
                        "lastModified": parse_date(row.get("hs_lastmodifieddate")),
                        "createdBy": row.get("hs_created_by_user_id"),
                        "engagementSource": row.get("hs_engagement_source"),
                        "sourceId": row.get("hs_engagement_source_id"),
                        "readOnly": row.get("hs_read_only"),
                        "sharedTeams": row.get("hs_shared_team_ids"),
                        "sharedUsers": row.get("hs_shared_user_ids"),
                        "productName": row.get("hs_product_name"),
                        "queueMembershipIds": row.get("hs_queue_membership_ids"),
                        "objCoords": row.get("hs_obj_coords"),
                        "mergedObjectIds": row.get("hs_merged_object_ids"),
                        "updatedBy": row.get("hs_updated_by_user_id"),
                    }
                }
            case "task":
                activity_doc = {
                    "type": row.get("hs_activity_type", "TASK").strip(),    
                    "contact": contact_id,                                   
                    "author": row.get("hs_created_by_user_id", None),            
                    "subject": row.get("hs_task_subject", "").strip(),           
                    "content": row.get("hs_task_body", "").strip(),              
                    "status": "Completed" if row.get("hs_task_is_completed") == "true" else "Pending", 
                    "dueDate": parse_date(row.get("hs_start_date")),
                    "externalId": row.get("EngagementId"),             
                    "source": "HubSpot",
                    "metadata": {
                        "vid": row.get("vid"),
                        "objectId": row.get("hs_object_id"),
                        "pipeline": row.get("hs_pipeline"),
                        "pipelineStage": row.get("hs_pipeline_stage"),
                        "taskType": row.get("hs_task_type"),
                        "taskCategory": row.get("hs_marketing_task_category"),
                        "taskCategoryId": row.get("hs_marketing_task_category_id"),
                        "priority": row.get("hs_task_priority"),
                        "probabilityToComplete": row.get("hs_task_probability_to_complete"),
                        "repeatStatus": row.get("hs_repeat_status"),
                        "scheduledTasks": row.get("hs_scheduled_tasks"),
                        "reminders": row.get("hs_task_reminders"),
                        "relativeReminders": row.get("hs_task_relative_reminders"),
                        "allOwnerIds": row.get("hs_all_owner_ids"),
                        "allTeamIds": row.get("hs_all_team_ids"),
                        "accessibleTeamIds": row.get("hs_all_accessible_team_ids"),
                        "assignedBusinessUnits": row.get("hs_all_assigned_business_unit_ids"),
                        "attachments": row.get("hs_attachment_ids"),
                        "lastModified": parse_date(row.get("hs_lastmodifieddate")),
                        "createdBy": row.get("hs_created_by_user_id"),
                        "engagementSource": row.get("hs_engagement_source"),
                        "sourceId": row.get("hs_engagement_source_id"),
                        "readOnly": row.get("hs_read_only"),
                        "sharedTeams": row.get("hs_shared_team_ids"),
                        "sharedUsers": row.get("hs_shared_user_ids"),
                        "productName": row.get("hs_product_name"),
                        "queueMembershipIds": row.get("hs_queue_membership_ids"),
                        "objCoords": row.get("hs_obj_coords"),
                        "mergedObjectIds": row.get("hs_merged_object_ids"),
                        "updatedBy": row.get("hs_updated_by_user_id"),
                        "wasImported": row.get("hs_was_imported"),
                        "teamId": row.get("hubspot_team_id"),
                        "ownerId": row.get("hubspot_owner_id"),
                    }
                }
            case _:
                print(f"⚠️ Unknown engagement type '{engagement_type}', skipping.")
                skipped_rows += 1
                continue

        operations.append(UpdateOne({"externalId": row.get("EngagementId")}, {"$set": activity_doc}, upsert=True))

    if dry_run:
        print(f"🧪 Dry run complete — {len(operations)} activities prepared, {skipped_rows} skipped.")
    else:
        if operations:
            print(f"🚀 Importing {len(operations)} activities to MongoDB...")
            result = activities_collection.bulk_write(operations)
            print(f"✅ {len(operations)} activities inserted successfully.")
            print(f"⚙️ Skipped: {skipped_rows}")
        else:
            print("⚠️ No valid activities to import.")
