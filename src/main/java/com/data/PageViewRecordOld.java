import com.clicktale.data.PageViewGenerated;
import com.clicktale.json.GenericJson;
import com.clicktale.utils.TimeUtils;
import com.clicktale.utils.Utils;
import com.google.gson.annotations.Expose;

import javax.annotation.Generated;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Gennady.Gilin on 6/19/2016.
 */

@Generated("org.jsonschema2pojo")
public class PageViewRecordOld extends GenericJson {

    @Expose
    private Long entityType;
    @Expose
    private Long projectId;
    @Expose
    private Long visitorId;
    @Expose
    private Long visitId;
    @Expose
    private Long pageviewId;
    @Expose
    private Long timestamp;
    @Expose
    private String location;

    @Expose
    private String referrer;

    @Expose
    private Long engagementTime;

    @Expose
    private Long domLoadTime;

    @Expose
    private Long jsErrorCount;

    @Expose
    private Long timeOnPage;

    @Expose
    private Long numOfClicks;

    @Expose
    private Double scrollReach;

    @Expose
    private Long durationSinceLastVisit;

    @Expose
    private Integer clientWidth;

    @Expose
    private Integer clientHeight;

    @Expose
    private String browser;

    @Expose
    private String browserWithVersion;

    @Expose
    private List<Integer> eventIds = new ArrayList<Integer>();


    public PageViewRecordOld(PageViewGenerated pageViewGenerated ){

        this.entityType = pageViewGenerated.getEntityType();
        this.projectId = pageViewGenerated.projectId;
        this.visitorId = pageViewGenerated.visitorId;
        this.visitId = pageViewGenerated.visitId;
        this.pageviewId = pageViewGenerated.pageviewId;
        this.timestamp = pageViewGenerated.timestamp;
        this.location = pageViewGenerated.location;
        this.referrer = pageViewGenerated.referrer;
        this.engagementTime = pageViewGenerated.engagementTime;
        this.domLoadTime = pageViewGenerated.domLoadTime;
        this.jsErrorCount = pageViewGenerated.jsErrorCount;
        this.timeOnPage = pageViewGenerated.timeOnPage;
        this.numOfClicks = pageViewGenerated.numOfClicks;
        this.scrollReach = pageViewGenerated.scrollReach;
        this.durationSinceLastVisit = pageViewGenerated.durationSinceLastVisit;
        this.clientWidth = pageViewGenerated.clientWidth;
        this.clientHeight = pageViewGenerated.clientHeight;
        this.browser = pageViewGenerated.browser;
        this.browserWithVersion = pageViewGenerated.browserWithVersion;
        this.eventIds = Utils.stringToListOfIntegers( pageViewGenerated.tags );
    }

    /**
     *
     * @return
     * The entityType
     */
    public Long getEntityType() {
        return entityType;
    }

    /**
     *
     * @param entityType
     * The entityType
     */
    public void setEntityType(Long entityType) {
        this.entityType = entityType;
    }

    /**
     *
     * @return
     * The projectId
     */
    public Long getProjectId() {
        return projectId;
    }

    /**
     *
     * @param projectId
     * The projectId
     */
    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }

    /**
     *
     * @return
     * The visitorId
     */
    public Long getVisitorId() {
        return visitorId;
    }

    /**
     *
     * @param visitorId
     * The visitorId
     */
    public void setVisitorId(Long visitorId) {
        this.visitorId = visitorId;
    }

    /**
     *
     * @return
     * The visitId
     */
    public Long getVisitId() {
        return visitId;
    }

    /**
     *
     * @param visitId
     * The visitId
     */
    public void setVisitId(Long visitId) {
        this.visitId = visitId;
    }

    /**
     *
     * @return
     * The pageviewId
     */
    public Long getPageviewId() {
        return pageviewId;
    }

    /**
     *
     * @param pageviewId
     * The pageviewId
     */
    public void setPageviewId(Long pageviewId) {
        this.pageviewId = pageviewId;
    }

    /**
     *
     * @return
     * The timestamp
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     *
     * @param timestamp
     * The timestamp
     */
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     *
     * @return
     * The location
     */
    public String getLocation() {
        return location;
    }

    /**
     *
     * @param location
     * The location
     */
    public void setLocation(String location) {
        this.location = location;
    }

    /**
     *
     * @return
     * The referrer
     */
    public String getReferrer() {
        return referrer;
    }

    /**
     *
     * @param referrer
     * The referrer
     */
    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    /**
     *
     * @return
     * The engagementTime
     */
    public Long getEngagementTime() {
        return engagementTime;
    }

    /**
     *
     * @param engagementTime
     * The engagementTime
     */
    public void setEngagementTime(Long engagementTime) {
        this.engagementTime = engagementTime;
    }

    /**
     *
     * @return
     * The domLoadTime
     */
    public Long getDomLoadTime() {
        return domLoadTime;
    }

    /**
     *
     * @param domLoadTime
     * The domLoadTime
     */
    public void setDomLoadTime(Long domLoadTime) {
        this.domLoadTime = domLoadTime;
    }

    /**
     *
     * @return
     * The jsErrorCount
     */
    public Long getJsErrorCount() {
        return jsErrorCount;
    }

    /**
     *
     * @param jsErrorCount
     * The jsErrorCount
     */
    public void setJsErrorCount(Long jsErrorCount) {
        this.jsErrorCount = jsErrorCount;
    }

    /**
     *
     * @return
     * The timeOnPage
     */
    public Long getTimeOnPage() {
        return timeOnPage;
    }

    /**
     *
     * @param timeOnPage
     * The timeOnPage
     */
    public void setTimeOnPage(Long timeOnPage) {
        this.timeOnPage = timeOnPage;
    }

    /**
     *
     * @return
     * The numOfClicks
     */
    public Long getNumOfClicks() {
        return numOfClicks;
    }

    /**
     *
     * @param numOfClicks
     * The numOfClicks
     */
    public void setNumOfClicks(Long numOfClicks) {
        this.numOfClicks = numOfClicks;
    }

    /**
     *
     * @return
     * The scrollReach
     */
    public Double getScrollReach() {
        return scrollReach;
    }

    /**
     *
     * @param scrollReach
     * The scrollReach
     */
    public void setScrollReach(Double scrollReach) {
        this.scrollReach = scrollReach;
    }

    /**
     *
     * @return
     * The durationSinceLastVisit
     */
    public Long getDurationSinceLastVisit() {
        return durationSinceLastVisit;
    }

    /**
     *
     * @param durationSinceLastVisit
     * The durationSinceLastVisit
     */
    public void setDurationSinceLastVisit(Long durationSinceLastVisit) {
        this.durationSinceLastVisit = durationSinceLastVisit;
    }

    /**
     *
     * @return
     * The clientWidth
     */
    public Integer getClientWidth() {
        return clientWidth;
    }

    /**
     *
     * @param clientWidth
     * The clientWidth
     */
    public void setClientWidth(Integer clientWidth) {
        this.clientWidth = clientWidth;
    }

    /**
     *
     * @return
     * The clientHeight
     */
    public Integer getClientHeight() {
        return clientHeight;
    }

    /**
     *
     * @param clientHeight
     * The clientHeight
     */
    public void setClientHeight(Integer clientHeight) {
        this.clientHeight = clientHeight;
    }

    /**
     *
     * @return
     * The browser
     */
    public String getBrowser() {
        return browser;
    }

    /**
     *
     * @param browser
     * The browser
     */
    public void setBrowser(String browser) {
        this.browser = browser;
    }

    /**
     *
     * @return
     * The browserWithVersion
     */
    public String getBrowserWithVersion() {
        return browserWithVersion;
    }

    /**
     *
     * @param browserWithVersion
     * The browserWithVersion
     */
    public void setBrowserWithVersion(String browserWithVersion) {
        this.browserWithVersion = browserWithVersion;
    }

    /**
     *
     * @return
     * The eventIds
     */
    public List<Integer> getEventIds() {
        return eventIds;
    }

    /**
     *
     * @param eventIds
     * The eventIds
     */
    public void setEventIds(List<Integer> eventIds) {
        this.eventIds = eventIds;
    }


    public static PageViewRecordOld fromString(String line ) {

        return fromString( line, PageViewRecordOld.class );
    }

    public String toEventsString(){

        StringBuilder builder = new StringBuilder();

        try{

            if( ( eventIds != null ) && ( eventIds.size() > 0 ) ){

                String eventHeader = entityType.toString() + "," + projectId + "," + visitorId + "," + visitId + "," + getPageviewId() + ",";

                for( int currentIndex = 0; currentIndex < eventIds.size(); currentIndex ++ ){

                    builder.append( eventIds.get( currentIndex ).toString() );
                    builder.append( eventHeader );
                    builder.append( TimeUtils.timestampToString( this.timestamp ));

                    if( currentIndex < ( eventIds.size() - 1 ) ){

                        builder.append( System.lineSeparator() );
                    }

                }
            }


        }catch( Exception ex ){

            ex.printStackTrace();
        }

        return builder.toString();
    }


    public String toRawString(){

        return super.toString();

    }

    @Override
    public String toString(){

        StringBuilder builder = new StringBuilder();

        try{

            builder.append( entityType.toString() );
            builder.append(",");
            builder.append( this.projectId );
            builder.append(",");
            builder.append( this.visitorId );
            builder.append(",");
            builder.append( this.visitId );
            builder.append(",");
            builder.append( this.getPageviewId() );
            builder.append(",");

            builder.append( TimeUtils.timestampToString( this.timestamp ));
            builder.append(",");
            builder.append( TimeUtils.currentDataToString() );
            builder.append(",");

            URL url = new URL( this.location );

            builder.append( url.getProtocol() );
            builder.append(",");
            builder.append( url.getHost() );
            builder.append(",");
            builder.append( url.getRef() );
            builder.append(",");
            builder.append( Utils.ListOfIntToString( eventIds )  );


        }catch( Exception ex ){

            ex.printStackTrace();
        }

        return builder.toString();
    }

    public static void main( String[] args ){

        String json = "{   \"entityType\": 0, \"projectId\": 1, \"visitorId\": 2, \"visitId\": 6,     \"pageviewId\": 12,     \"timestamp\": 1457949445,     \"location\": \"http://location\",     \"referrer\": \"http://referrer\",     \"engagementTime\": 1,     \"domLoadTime\": 4,                 \"jsErrorCount\": 3,                 \"timeOnPage\": 23,                 \"numOfClicks\": 12,                 \"scrollReach\": 4.57,                 \"durationSinceLastVisit\": 157,                 \"clientWidth\": 1024,                 \"clientHeight\": 780,                 \"browser\": \"Daniel Is The King\",                 \"browserWithVersion\": \"DITK 1.7\",                 \"eventIds\": [1,2,3,4] }";
        PageViewRecordOld pageViewRecord = PageViewRecordOld.fromString( json );

        System.out.println( pageViewRecord.toEventsString() );
        System.out.println( pageViewRecord.toString() );
    }


}