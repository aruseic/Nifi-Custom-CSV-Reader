package com.collins.customcsvreader;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

/**
 * A simple validator to check that the separator field is configured correctly.
 */
public class SeparatorValidator implements Validator{

    final char[] separators;
    String allowedSeparatorsText;

    public SeparatorValidator(final char[] separators) {
        super();
        if(separators == null || separators.length ==0){
            throw new IllegalArgumentException("The separators array is required and must not be null or empty");
        }
        this.separators = separators;
        for(char c : separators){
            this.allowedSeparatorsText += String.format("%c ", c);
        }
    }

	@Override
	public ValidationResult validate(String subject, String input, ValidationContext context) {
        ValidationResult.Builder builder = new ValidationResult.Builder()
            .subject(subject)
            .input(input);
        //ignore support for escaped chars for now cause we don't need them
		if(input == null || input.isEmpty() || input.length() != 1){
            builder.valid(false) 
                .explanation("The separator is required and must be a single character e.g. |");
        }
        
        else if(input.length() == 1){
            //unescaped charater literal
            if(isvalid((input.charAt(0)))){
                builder.valid(true);
            } else {
                builder.valid(false)
                    .explanation("The separator is not in the list of allowed separators: " + allowedSeparatorsText);
            }
        }
        return builder.build();
    }
    
    private boolean isvalid(char sep){
        for(int i=0; i<separators.length; i++){
            if(separators[i] == sep){
                return true;
            }
        }
        return false;
    }

}