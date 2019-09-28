package com.sbn.italianref;

public class UserModel {
    String userName;
    String support;
    String userId;
    double hubnessAuthority;
    double supportScore;

    double CentralityScore;

    public double getCentralityScore() {
        return CentralityScore;
    }

    public void setCentralityScore(double centralityScore) {
        CentralityScore = centralityScore;
    }

    public double getHubnessAuthority() {
        return hubnessAuthority;
    }

    public void setHubnessAuthority(double hubnessAuthority) {
        this.hubnessAuthority = hubnessAuthority;
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
        return supportScore;
    }

    public void setSupportScore(double supportScore) {
        this.supportScore = supportScore;
        if(supportScore > 0) {
            support = "Yes";
        } else if(supportScore < 0) {
            support = "No";
        } else {
            support = "Neutral";
        }
    }

}
