import mongoose, { Schema } from "mongoose";

const companySchema = new Schema(
  {
    name: {
      type: String,
      required: true,
      trim: true,
    },

    domain: {
      type: String,
      trim: true,
      lowercase: true,
    },

    website: {
      type: String,
      trim: true,
    },

    industry: {
      type: String,
      trim: true,
    },

    about: {
      type: String,
      trim: true,
    },

    description: {
      type: String,
      trim: true,
    },

    foundedYear: Number,
    numberOfEmployees: Number,
    annualRevenue: Number,
    revenueCurrency: { type: String, trim: true },

    address: {
      line1: String,
      line2: String,
      city: String,
      state: String,
      zip: String,
      country: String,
    },

    phone: { type: String, trim: true },
    timezone: { type: String, trim: true },

    // --- Relationships ---
    contacts: [
      {
        type: Schema.Types.ObjectId,
        ref: "Contact",
      },
    ],

    owner: {
      type: Schema.Types.ObjectId,
      ref: "User",
    },

    // --- HubSpot Metadata ---
    meta: {
      companyId: String,
      createdAt: Date,
      updatedAt: Date,
      lifecycleStage: String,
      score: Number,
      lastContacted: Date,
      lastActivity: Date,
      source: String,
      notes: {
        lastUpdated: Date,
        nextActivityDate: Date,
      },
      analytics: {
        numPageViews: Number,
        numVisits: Number,
        latestSource: String,
        latestSourceData1: String,
        latestSourceData2: String,
      },
      social: {
        linkedin: String,
        twitter: String,
        facebook: String,
      },
    },
  },
  {
    timestamps: true,
  }
);

export const Company = mongoose.model("Company", companySchema);
