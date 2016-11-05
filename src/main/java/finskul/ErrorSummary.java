package finskul;

public class ErrorSummary extends Throwable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2630721203445555289L;
	
	public ErrorSummary(Throwable t)
	{
		super(t);
	}
	
	public ErrorSummary(Exception e)
	{
		super(e);
	}
	
	public ErrorSummary(String message)
	{
		super(message);
	}
	
	public ErrorSummary(String message, Exception e)
	{
		super(message, e);
	}
	
	public ErrorSummary(String message, Throwable t)
	{
		super(message, t);
	}
	
	public static String stackTrace(Throwable t)
	{
		StringBuilder builder = new StringBuilder();
		StackTraceElement elementArr[] = t.getStackTrace();
		for (int i = 0; i < elementArr.length; i++) {
			builder.append(elementArr[i].getFileName()).
			append(".").append(elementArr[i].getMethodName()).
			append("[").append(elementArr[i].getLineNumber()).append("]");
		}
		return builder.toString();
	}

}
