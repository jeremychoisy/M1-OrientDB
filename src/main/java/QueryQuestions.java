import Client.OrientDBClient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


class QueryQuestions {


    String query;

    OrientDBClient orient = new OrientDBClient(false);
    ODatabaseSession db = orient.getDB();

    public static void main(String[] args) {
        QueryQuestions qq = new QueryQuestions();
        String clientId = "2199023256728";
        String productId = "B005FUKW6M";
        String marque = "Franklin Sports";
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        Date result = cal.getTime();
        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd ");
        String date = formatter.format(result);
        //qq.question1("4145", date);
        //qq.question2(productId, "2019");
        //qq.question3("B003D9RBMU");
        //qq.question4();
        //qq.question5(clientId, marque);
        //qq.question6(clientId, "4398046521398");
        //qq.question7(marque);
        //qq.question8();
        //qq.question9();
        qq.question10();

    }

    /* For a given customer,
     find his/her all related data including profile,
     orders, invoices, feedback, comments, and posts in the last month,
     return the category in which he/she has bought the largest number of products,
     and return the tag which he/she has engaged the greatest times in the posts.
    */
    void question1(String clientId, String date) {
        query = "Select $profile, $posts, $orders, $feedback, $invoices "
        + "let $profile=(select birthday, lastName, gender, browserUsed, creationDate, firstName, locationIP, id, place from `Person` where id=?),"
        + "$posts= (select in(PostWasCreatedByPerson) from `Person` where id=?),"
        + "$orders=(select Orderline.productId from `Order` where PersonId=? AND OrderDate > ? ),"
        + "$feedback=(select feedback From Feedback where personId=?),"
        + "$invoices=(select Orderline.productId From Invoice where PersonId=? AND OrderDate > ?);" ;

        OResultSet rs = db.query(query, clientId, clientId, clientId, date, clientId, clientId, date);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /* For a given product during a given period, find the people who commented or posted on it, and had bought it. */
    void question2(String productId, String year) {
        query = "SELECT * From `Person` WHERE id IN (" +
                "SELECT PersonId FROM `Order` where Orderline.asin CONTAINS ? AND OrderDate.left(4) = ?)" +
                "AND (id " +
                "IN (SELECT personId FROM `Feedback` WHERE productId = ?) " +
                "OR id " +
                "IN (SELECT id FROM `Post`));";

        //OResultSet rs = db.query(query, productId, year, productId);
        OResultSet rs = db.query(query, productId, year, productId);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    For a given product during a given period,
     find people who have undertaken activities related to it, e.g.,
     posts, comments, and review,
     and return sentences from these texts that contain negative sentiments.
     */

    void question3(String productId) {
        query = "Select $post, $feedback "
                + "let $post=(SELECT language, id, creationDate ,content from `Product` WHERE asin IN(SELECT productId FROM `Feedback` WHERE productId = ?)),"
                + "$feedback=(select feedback, personId from `Feedback` where productId= ? and feedback.charAt(1) < 3);";

        OResultSet rs = db.query(query, productId, productId);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }

    /*
    Find the top-2 persons who spend the highest amount of money in orders.
    Then for each person, traverse her knows-graph with 3-hop to find the friends,
    and finally return the common friends of these two persons.
     */
    void question4() {
        query = "select PersonId, sum(TotalPrice) as spend from `Order` Group by PersonId order by spend desc limit 2";
        OResultSet rs = db.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }

    /*
     Given a start customer and a product category,
     find persons who are this customer's friends within 3-hop friendships in Knows graph,
     besides, they have bought products in the given category.
     Finally, return feedback with the 5-rating review of those bought products.
     */
    void question5(String clientId, String marque) {
        String query = "Select Out(\'PersonHasPost\').Out(\'PostHasTag\') " +
                "as tags from (select Expand(Out(\'Knows\')) from Customer where id=?) " +
                "Where ? in Order.Orderline.brand unwind tags";

        OResultSet rs = db.query(query, clientId, marque);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    Given customer 1 and customer 2, find persons in the shortest path between them in the subgraph,
    and return the TOP 3 best sellers from all these persons' purchases.
     */

    void question6(String clientId1, String clientId2) {
        query = "SELECT transactions, count(transactions) as cnt "
                + "FROM(SELECT Order.Orderline.productId as transactions from(SELECT EXPAND(path) from(SELECT shortestPath($from, $to) AS path "
                + "LET $from = (SELECT FROM Customer WHERE id=?),"
                + "$to = (SELECT FROM Customer WHERE id=?))) unwind transactions) GROUP BY transactions Order by cnt DESC LIMIT 5";
        OResultSet rs = db.query(query, clientId1, clientId2);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    For the products of a given vendor with declining sales compare to the former quarter,
     analyze the reviews for these items to see if there are any negative sentiments.
     */
    void question7() {
        query = "";
        OResultSet rs = db.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    For all the products of a given category during a given year, compute its total sales amount,
    and measure its popularity in the social media.
    */
    void question8() {
        query = "Select Sum(Popularity) from(Select In(\'PostHasTag\').size() as Popularity "
                + "from `Product` Where productId in (Select  Distinct(Orderline.productId) "
                + "From (Select Orderline From Order let  $brand=(select name as brand from `Vendor` where country='China') "
                + "Where OrderDate.left(4)=2018 unwind Orderline) Where Orderline.brand in $brand.brand))";
        OResultSet rs = db.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }
    /*
    Find top-3 companies who have the largest amount of sales at one country, for each company,
    compare the number of the male and female customers, and return the most recent posts of them.
     */
    void question9() {
        query = "Select Orderline.brand, count(*) from(Select PersonId, Orderline From Order "
                + "Let $brand=(select name as brand from `Vendor` where Country='China') Where OrderDate.left(4)=2019 unwind Orderline) "
                + "Where Orderline.brand in $brand.brand Group by Orderline.brand Order by count DESC LIMIT 3";

        OResultSet rs = db.query(query);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    Find the top-10 most active persons by aggregating the posts during the last year,
    then calculate their RFM (Recency, Frequency, Monetary) value in the same period,
     and return their recent reviews and tags of interest.
     */
    void question10() {
        query = "SELECT PersonId, OrderId, max(OrderDate) as Recency, sum(TotalPrice) as Monetary FROM Order "
                + "Where PersonId IN" +
                "(" +
                "Select OUT(\'PostWasCreatedByPerson\').id[0] as id From Post Where creationDate>= '2008-01-01'" +
                ");";

        OResultSet rs = db.query(query);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
    }
}