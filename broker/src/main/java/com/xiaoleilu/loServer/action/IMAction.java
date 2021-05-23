/*
 * This file is part of the Wildfire Chat package.
 * (c) Heavyrain2012 <heavyrain.lee@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */


package com.xiaoleilu.loServer.action;

import cn.wildfirechat.proto.WFCMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.xiaoleilu.loServer.annotation.HttpMethod;
import com.xiaoleilu.loServer.annotation.Route;
import com.xiaoleilu.loServer.handler.Request;
import com.xiaoleilu.loServer.handler.Response;
import io.moquette.persistence.ServerAPIHelper;
import io.moquette.persistence.MemorySessionStore;
import io.moquette.spi.impl.Utils;
import io.moquette.spi.impl.security.AES;
import io.moquette.spi.security.Tokenor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import cn.wildfirechat.common.ErrorCode;

import java.util.Base64;
import java.util.concurrent.Executor;

@Route("/im")
@HttpMethod("POST")
public class IMAction extends Action {

    @Override
    public boolean action(Request request) {
        if (request.getNettyRequest() instanceof FullHttpRequest) {

            FullHttpRequest fullHttpRequest = (FullHttpRequest)request.getNettyRequest();
            byte[] bytes = Utils.readBytesAndRewind(fullHttpRequest.content());

            String str = new String(bytes);
            try {
                bytes = Base64.getDecoder().decode(str);
            } catch (IllegalArgumentException e) {
                sendResponse(response, ErrorCode.ERROR_CODE_SECRECT_KEY_MISMATCH, null);
                return true;
            }

            String cid = fullHttpRequest.headers().get("cid");
            byte[] cbytes = Base64.getDecoder().decode(cid);
            cbytes = AES.AESDecrypt(cbytes, "", true);
            if (cbytes == null) {
                sendResponse(response, ErrorCode.ERROR_CODE_SECRECT_KEY_MISMATCH, null);
                return true;
            }
            cid = new String(cbytes);

            MemorySessionStore.Session session = sessionsStore.getSession(cid);

            if (session != null) {
                bytes = AES.AESDecrypt(bytes, session.getSecret(), true);
            } else {
                sendResponse(response, ErrorCode.ERROR_CODE_SECRECT_KEY_MISMATCH, null);
                return true;
            }


            if (bytes == null) {
                sendResponse(response, ErrorCode.ERROR_CODE_SECRECT_KEY_MISMATCH, null);
                return true;
            }

            try {
                WFCMessage.IMHttpWrapper wrapper = WFCMessage.IMHttpWrapper.parseFrom(bytes);
                String token = wrapper.getToken();
                String userId = Tokenor.getUserId(token.getBytes());
                if (userId == null) {
                    sendResponse(response, ErrorCode.ERROR_CODE_TOKEN_ERROR, null);
                } else {
                    ServerAPIHelper.sendRequest(userId, wrapper.getClientId(), wrapper.getRequest(), wrapper.getData().toByteArray(), new ServerAPIHelper.Callback() {
                        @Override
                        public void onSuccess(byte[] result) {
                            sendResponse(response, null, result);
                        }

                        @Override
                        public void onError(ErrorCode errorCode) {
                            sendResponse(response, errorCode, null);
                        }

                        @Override
                        public void onTimeout() {
                            sendResponse(response, ErrorCode.ERROR_CODE_TIMEOUT, null);
                        }

                        @Override
                        public Executor getResponseExecutor() {
                            return command -> {
                                ctx.executor().execute(command);
                            };
                        }
                    }, false);
                    return false;
                }
            } catch (InvalidProtocolBufferException e) {
                sendResponse(response, ErrorCode.ERROR_CODE_INVALID_DATA, null);
            }
        }
        return true;
    }

    private void sendResponse(Response response, ErrorCode errorCode, byte[] contents) {
        response.setStatus(HttpResponseStatus.OK);
        if(contents == null) {
            ByteBuf ackPayload = Unpooled.buffer();
            ackPayload.ensureWritable(1).writeByte(errorCode.getCode());
            response.setContent(ackPayload);
        } else {
            response.setContent(contents);
        }
        response.send();

        if ("close".equals(request.getHeader("Connection"))) {
            ctx.flush();
            ctx.close();
        }
    }
}
