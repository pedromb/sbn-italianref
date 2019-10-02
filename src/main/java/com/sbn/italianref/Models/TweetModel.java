package com.sbn.italianref.Models;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class TweetModel {

    private String User;
    private String UserCreatedTweet;
    private String UserSupport;
    private String UserCreatedTweetSupport;
    private String ActualSupport;
    private LocalDateTime CreatedAt;
    private long CreatedAtTimestamp;
    private int DocId;
    private String Text;
    private Map<String, Long> TermsVector;
    private String UserId;

    public String getUserId() {
        return UserId;
    }

    public void setUserId(String userId) {
        UserId = userId;
    }

    public int getDocId() {
        return DocId;
    }

    public void setDocId(int docId) {
        DocId = docId;
    }

    public long getCreatedAtTimestamp() {
        return CreatedAtTimestamp;
    }

    public void setCreatedAtTimestamp(long createdAtTimestamp) {
        CreatedAtTimestamp = createdAtTimestamp;
    }

    public Map<String, Long> getTermsVector() {
        return TermsVector;
    }

    public void setTermsVector(Map<String, Long> termsVector) {
        TermsVector = termsVector;
    }

    public String getText() {
        return Text;
    }

    public void setText(String text) {
        Text = text;
    }


    public String getUser() {
        return User;
    }

    public void setUser(String user) {
        User = user;
    }

    public String getUserCreatedTweet() {
        return UserCreatedTweet;
    }

    public void setUserCreatedTweet(String userCreatedTweet) {
        UserCreatedTweet = userCreatedTweet;
    }

    public String getUserSupport() {
        return UserSupport;
    }

    public void setUserSupport(String userSupport) {
        UserSupport = userSupport;
    }

    public String getUserCreatedTweetSupport() {
        return UserCreatedTweetSupport;
    }

    public void setUserCreatedTweetSupport(String userCreatedTweetSupport) {
        UserCreatedTweetSupport = userCreatedTweetSupport;
    }

    public String getActualSupport() {
        return ActualSupport;
    }

    public void setActualSupport(String actualSupport) {
        ActualSupport = actualSupport;
    }

    public LocalDateTime getCreatedAt() {
        return CreatedAt;
    }

    public LocalDateTime getCreatedAtTruncatedHours() {
        return CreatedAt.truncatedTo(ChronoUnit.HOURS);
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        CreatedAt = createdAt;
    }


}
