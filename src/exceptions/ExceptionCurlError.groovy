package exceptions
/**
 * Created by igiagante on 8/5/15.
 */
class ExceptionCurlError extends Exception{

    private static final long serialVersionUID = 4664456874499611218L;

    private String errorCode = "Unknown_Exception";

    public ExceptionCurlError(String message){
        super(message);
        this.errorCode = errorCode;
    }

    public ExceptionCurlError(String message, String errorCode){
        super(message);
        this.errorCode = errorCode;
    }

    public String getErrorCode(){
        return this.errorCode;
    }
}
