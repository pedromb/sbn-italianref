package com.sbn.italianref.Models;

public class UserModel {
    String userName;
    String support;
    String userId;
    double hubScore;
    double authorityScore;
    double supportScore;
    double centralityScore;


    public double getAuthorityScore() {
        return authorityScore;
    }

    public void setAuthorityScore(double authorityScore) {
        this.authorityScore = authorityScore;
    }

    public double getCentralityScore() {
        return centralityScore;
    }

    public void setCentralityScore(double centralityScore) {
        this.centralityScore = centralityScore;
    }

    public double getHubScore() {
        return hubScore;
    }

    public void setHubScore(double hubScore) {
        this.hubScore = hubScore;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getSupport() {
        return support;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getSupportScore() {
        return this.supportScore;
    }

    public void setSupportScore(double supportScore) {
        this.supportScore = supportScore;
        if(supportScore > 10) {
            support = "Yes";
        } else if(supportScore < -10) {
            support = "No";
        } else {
            support = "Neutral";
        }
    }

}
