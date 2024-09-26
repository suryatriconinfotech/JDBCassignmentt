import java.sql.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:mysql://localhost:3306/jdbcdemo";
        String username = "root";
        String password = "root";
        Connection connection = DriverManager.getConnection(url, username, password);

        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);

            String bookName = "Harry Potter and the Chamber of Secrets";
            String authorName = "J.K. Rowling";


            String selectAuthorSql = "SELECT id, no_of_books_wrote FROM authors WHERE name = ?";
            PreparedStatement selectAuthorStmt = connection.prepareStatement(selectAuthorSql);
            selectAuthorStmt.setString(1, authorName);
            ResultSet authorResult = selectAuthorStmt.executeQuery();

            int authorId;
            if (authorResult.next()) {

                authorId = authorResult.getInt("id");
                int booksWritten = authorResult.getInt("no_of_books_wrote");

                String updateAuthorSql = "UPDATE authors SET no_of_books_wrote = ? WHERE id = ?";
                PreparedStatement updateAuthorStmt = connection.prepareStatement(updateAuthorSql);
                updateAuthorStmt.setInt(1, booksWritten + 1);  // Increment book count
                updateAuthorStmt.setInt(2, authorId);
                updateAuthorStmt.executeUpdate();
                System.out.println("Updated existing author: " + authorName);

            } else {

                String insertAuthorSql = "INSERT INTO authors (name, no_of_books_wrote) VALUES (?, ?)";
                PreparedStatement insertAuthorStmt = connection.prepareStatement(insertAuthorSql, Statement.RETURN_GENERATED_KEYS);
                insertAuthorStmt.setString(1, authorName);
                insertAuthorStmt.setInt(2, 1);
                insertAuthorStmt.executeUpdate();


                ResultSet generatedKeys = insertAuthorStmt.getGeneratedKeys();
                if (generatedKeys.next()) {
                    authorId = generatedKeys.getInt(1);
                } else {
                    throw new SQLException("Failed to insert author, no ID obtained.");
                }
                System.out.println("Inserted new author: " + authorName);
            }


            String insertBookSql = "INSERT INTO books (name, author) VALUES (?, ?)";
            PreparedStatement insertBookStmt = connection.prepareStatement(insertBookSql, Statement.RETURN_GENERATED_KEYS);
            insertBookStmt.setString(1, bookName);
            insertBookStmt.setString(2, authorName);
            insertBookStmt.executeUpdate();


            ResultSet bookKeys = insertBookStmt.getGeneratedKeys();
            int bookId;
            if (bookKeys.next()) {
                bookId = bookKeys.getInt(1);
            } else {
                throw new SQLException("Failed to insert book, no ID obtained.");
            }
            System.out.println("Inserted new book: " + bookName);


            String insertBookAuthorSql = "INSERT INTO book_author (book_id, author_id) VALUES (?, ?)";
            PreparedStatement insertBookAuthorStmt = connection.prepareStatement(insertBookAuthorSql);
            insertBookAuthorStmt.setInt(1, bookId);
            insertBookAuthorStmt.setInt(2, authorId);
            insertBookAuthorStmt.executeUpdate();


            connection.commit();
            System.out.println("Transaction committed successfully.");

        } catch (SQLException e) {
            System.out.println("Error occurred: " + e.getMessage());
            connection.rollback();
            System.out.println("Transaction rolled back.");
        } finally {
            connection.setAutoCommit(autoCommit);
            connection.close();
        }
    }
}
