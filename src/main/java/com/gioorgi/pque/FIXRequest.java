package com.gioorgi.pque;


import lombok.*;
import org.hibernate.validator.constraints.Length;

import javax.persistence.Embeddable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/* 
 * Real-FIX44 transfer object, simplified.
 * 
 */
@Data
@Builder
@EqualsAndHashCode
@AllArgsConstructor
@RequiredArgsConstructor
@Embeddable
public class FIXRequest {
    
    public enum FixMessageType  {

        NEW_ORDER_SINGLE("D"), //
        QUOTE("S"), //
    
        EXECUTION_REPORT("8"), //
        QUOTE_CANCEL("Z"), //
        QUOTE_REJECT("AG"), //
        QUOTE_REQUEST("R"), //
    
        UNKNOWN("??");
    
        private final String id;
    
        FixMessageType(String id) {
            this.id = id;
        }
    
        public String id() {
            return id;
        }
    }
    
    public enum OrdType  {

        LIMIT('2'),
    
        MARKET('1'),
    
        PREVIOUSLY_QUOTED('D');
    
        private final char id;
    
        OrdType(char id) {
            this.id = id;
        }

        public static OrdType fromMarket(char value) {
            for(var o:OrdType.values()){
                if (o.id()==value){
                    return o;
                }
            }
            throw new IllegalArgumentException("Unknown ord type:"+value);
        }

        public char id() {
            return id;
        }
    }

    public enum Side {

        BUY("1"), //
        SELL("2"), //
        NONE("-");
    
        private final String id;
    
        Side(String id) {
            this.id = id;
        }    
        
        public char id() {
            return id.charAt(0);
        }

        public static Side fromMarket( char c) throws IllegalArgumentException {
            switch(c) {
                case '1': return Side.BUY;
                case '2': return Side.SELL;
                case '-': return Side.NONE;
                default:
                    throw new IllegalArgumentException("Unknown side:"+c);
            }
        }
        public static Side parse(@NotNull String fixStr) {
            if(fixStr.equalsIgnoreCase("BUY")){
                return Side.BUY;
            }else if(fixStr.equalsIgnoreCase("SELL")){
                return Side.SELL;
            }else{
                return Side.NONE;
            }
        }
    }

    String quoteReqId; // i.e. "req_quote";

    /** i.e. EURAMD
     *
     */    
    @Length(min = 6, max=6)
    @NotEmpty
    String symbol;

    @NotNull
    @Pattern(regexp="SP|SPOT|INDIVIDUAL", message="settlement mode  must be SP, SPOT or INDIVIDUAL")
    String settlType; // Refer to ValueDating, possible values are "SP,INDIVIDUAL"

    @NotNull
    BigDecimal quantity;

    /**
     * i.e. 20230612
     */
    @NotNull
    String settlDate;

    LocalDateTime transactTime;

    LocalDateTime validUntilTime;

    @Pattern(regexp="[YN]", message="dailyFlag must be only N or Y")
    String dailyFlag;   // i.e. Y

    public boolean isDailyRequest(){
        // Mus start with DR too
        return "Y".equals(dailyFlag);
    }



    @Pattern(regexp="[YN]", message="Only exotic flag right now")
    String exoticFlag;  // i.e. Y

    @NotNull(message = "Message type is mandatory")
    FixMessageType msgType; 

    // Extra filed for NewOrderSingle (nos) ///////////////////////////////////////////////////////////////////////
    Side side;


    String clOrdId;

    String secondaryClOrdId;
 

    String quoteId; // Can be null
    OrdType ordType ; 

 


}
 