/*
 * Copyright (c) 2012-2017 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.persistence;

import cn.wildfirechat.pojos.*;
import cn.wildfirechat.proto.ProtoConstants;
import cn.wildfirechat.proto.WFCMessage;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.hazelcast.core.*;
import com.hazelcast.util.StringUtil;
import com.xiaoleilu.loServer.model.FriendData;
import cn.wildfirechat.common.ErrorCode;
import io.moquette.BrokerConstants;
import io.moquette.imhandler.IMHandler;
import io.moquette.server.Constants;
import io.moquette.server.Server;
import io.moquette.spi.IMatchingCondition;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.security.Tokenor;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.liyufan.im.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static cn.wildfirechat.common.IMExceptionEvent.EventType.EVENT_CALLBACK_Exception;
import static cn.wildfirechat.proto.ProtoConstants.ChannelState.Channel_State_Mask_Deleted;
import static cn.wildfirechat.proto.ProtoConstants.ChannelUpdateEventType.*;
import static cn.wildfirechat.proto.ProtoConstants.ChatroomMemberUpdateEventType.Chatroom_Member_Event_Join;
import static cn.wildfirechat.proto.ProtoConstants.ChatroomMemberUpdateEventType.Chatroom_Member_Event_Leave;
import static cn.wildfirechat.proto.ProtoConstants.ChatroomUpdateEventType.Chatroom_Event_Create;
import static cn.wildfirechat.proto.ProtoConstants.ChatroomUpdateEventType.Chatroom_Event_Destroy;
import static cn.wildfirechat.proto.ProtoConstants.GroupMemberType.*;
import static cn.wildfirechat.proto.ProtoConstants.ModifyChannelInfoType.*;
import static cn.wildfirechat.proto.ProtoConstants.ModifyGroupInfoType.*;
import static cn.wildfirechat.proto.ProtoConstants.PersistFlag.Transparent;
import static cn.wildfirechat.proto.ProtoConstants.Platform.*;
import static cn.wildfirechat.proto.ProtoConstants.UpdateUserInfoMask.*;
import static io.moquette.BrokerConstants.*;
import static io.moquette.server.Constants.MAX_CHATROOM_MESSAGE_QUEUE;
import static io.moquette.server.Constants.MAX_MESSAGE_QUEUE;
import static cn.wildfirechat.pojos.MyInfoType.*;
import static win.liyufan.im.UserSettingScope.kUserSettingPCOnline;

public class MemoryMessagesStore implements IMessagesStore {
    private static final String MESSAGES_MAP = "messages_map";
    private static final String GROUPS_MAP = "groups_map";
    static int dumy = 0;
    static final String GROUP_MEMBERS = "group_members";

    private static final String CHATROOM_MEMBER_IDS = "chatroom_members";
    private static final String USER_CHATROOM = "user_chatroom";

    static final String USER_FRIENDS = "user_friends";
    static final String USER_FRIENDS_REQUEST = "user_friends_request";

    static final String USERS = "users";

    static final String USER_STATUS = "user_status";

    private static final String CHATROOMS = "chatrooms";

    private static final String USER_SETTING = "user_setting";

    private static final String CHANNELS = "channels_map";

    private static final String CHANNEL_LISTENERS = "channel_listeners";

    private static boolean IS_MESSAGE_ROAMING = true;

    private static boolean IS_MESSAGE_REMOTE_HISTORY_MESSAGE = true;

    static final String USER_ROBOTS = "user_robots";
    static final String USER_THINGS = "user_things";

    static final String ROBOTS = "robots";
    static final String THINGS = "things";

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMessagesStore.class);

    private Map<Topic, StoredMessage> m_retainedStore = new HashMap<>();

    private final Server m_Server;

    private Map<String, TreeMap<Long, Long>> userMessages = new HashMap<>();

    private Map<String, TreeMap<Long, Long>> chatroomMessages = new HashMap<>();

    private ReadWriteLock mLock = new ReentrantReadWriteLock();
    private Lock mReadLock = mLock.readLock();
    private Lock mWriteLock = mLock.writeLock();

    private final DatabaseStore databaseStore;
    private ConcurrentHashMap<String, Long> userMaxPullSeq = new ConcurrentHashMap<>();

    private SensitiveFilter mSensitiveFilter;
    private volatile long lastUpdateSensitiveTime = 0;

    private ConcurrentHashMap<String, Boolean> userGlobalSlientMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Boolean> userPushHiddenDetail = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Boolean> userConvSlientMap = new ConcurrentHashMap<>();

    private boolean mDisableSearch = false;
    private boolean mDisableNicknameSearch = false;
    private boolean mDisableFriendRequest = false;
    private long mFriendRequestDuration = 7 * 24 * 60 * 60 * 1000;
    private long mFriendRejectDuration = 30 * 24 * 60 * 60 * 1000;
    private long mFriendRequestExpiration = 7 * 24 * 60 * 60 * 1000;
    private boolean mFriendNewWelcomeMessage = true;

    private boolean mMultiPlatformNotification = false;
    private boolean mDisableStrangerChat = false;

    private long mChatroomParticipantIdleTime = 900000;
    private boolean mChatroomRejoinWhenActive = true;

    private List<Integer> mForbiddenClientSendTypes = new ArrayList<>();
    private List<Integer> mBlackListExceptionTypes = new ArrayList<>();
    private List<Integer> mGroupMuteExceptionTypes = new ArrayList<>();
    private List<Integer> mGlobalMuteExceptionTypes = new ArrayList<>();

    private long mRecallTimeLimit = 300;
    private boolean mDisableGroupManagerRecall = false;
    private String mGroupInfoUpdateCallback;
    private String mGroupMemberUpdateCallback;
    private String mRelationUpdateCallback;
    private String mUserInfoUpdateCallback;
    private String mChannelInfoUpdateCallback;
    private String mChatroomInfoUpdateCallback;
    private String mChatroomMemberUpdateCallback;
    private boolean mGroupAllowClientCustomOperationNotification;
    private int mGroupVisibleQuitKickoffNotification;
    private int mSyncDataPartSize = 0;

    private Set<Integer> mUserHideProperties = new HashSet<>();

    MemoryMessagesStore(Server server, DatabaseStore databaseStore) {
        m_Server = server;
        this.databaseStore = databaseStore;

        IS_MESSAGE_ROAMING = "1".equals(m_Server.getConfig().getProperty(MESSAGE_ROAMING));
        IS_MESSAGE_REMOTE_HISTORY_MESSAGE = "1".equals(m_Server.getConfig().getProperty(MESSAGE_Remote_History_Message));
        Constants.MAX_MESSAGE_QUEUE = Integer.parseInt(m_Server.getConfig().getProperty(MESSAGE_Max_Queue));

        try {
            mDisableStrangerChat = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.MESSAGE_Disable_Stranger_Chat));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(MESSAGE_Disable_Stranger_Chat, mDisableStrangerChat + "");
        }

        try {
            mMultiPlatformNotification = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.SERVER_MULTI_PLATFROM_NOTIFICATION, "false"));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(SERVER_MULTI_PLATFROM_NOTIFICATION, mMultiPlatformNotification + "");
        }

        try {
            mDisableSearch = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.FRIEND_Disable_Search));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(FRIEND_Disable_Search, mDisableSearch + "");
        }
        try {
            mDisableNicknameSearch = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.FRIEND_Disable_NickName_Search));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(FRIEND_Disable_NickName_Search, mDisableNicknameSearch + "");
        }
        try {
            mDisableFriendRequest = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.FRIEND_Disable_Friend_Request));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(FRIEND_Disable_Friend_Request, mDisableFriendRequest + "");
        }

        try {
            mFriendRequestDuration = Long.parseLong(m_Server.getConfig().getProperty(FRIEND_Repeat_Request_Duration));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(FRIEND_Repeat_Request_Duration, mFriendRequestDuration + "");
        }

        try {
            mFriendRejectDuration = Long.parseLong(m_Server.getConfig().getProperty(FRIEND_Reject_Request_Duration));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(FRIEND_Reject_Request_Duration, mFriendRejectDuration + "");
        }

        try {
            mFriendRequestExpiration = Long.parseLong(m_Server.getConfig().getProperty(FRIEND_Request_Expiration_Duration));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(FRIEND_Request_Expiration_Duration, mFriendRequestExpiration + "");
        }

        try {
            mFriendNewWelcomeMessage = Boolean.parseBoolean(m_Server.getConfig().getProperty(FRIEND_New_Welcome_Message));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(FRIEND_New_Welcome_Message, mFriendNewWelcomeMessage + "");
        }


        try {
            mChatroomRejoinWhenActive = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.CHATROOM_Rejoin_When_Active));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(CHATROOM_Rejoin_When_Active, mChatroomRejoinWhenActive + "");
        }

        try {
            mChatroomParticipantIdleTime = Long.parseLong(m_Server.getConfig().getProperty(CHATROOM_Participant_Idle_Time, "900000"));
        } catch (Exception e) {
            e.printStackTrace();
            Utility.printExecption(LOG, e);
            printMissConfigLog(CHATROOM_Participant_Idle_Time, mChatroomParticipantIdleTime + "");
        }

        try {
            boolean disableRemoteMessageSearch = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.MESSAGES_DISABLE_REMOTE_SEARCH, "false"));
            databaseStore.setDisableRemoteMessageSearch(disableRemoteMessageSearch);
        } catch (Exception e) {
        }

        try {
            mRecallTimeLimit = Long.parseLong(m_Server.getConfig().getProperty(BrokerConstants.MESSAGES_RECALL_TIME_LIMIT));
        } catch (Exception e) {
        }

        try {
            mDisableGroupManagerRecall = Boolean.parseBoolean(m_Server.getConfig().getProperty(BrokerConstants.MESSAGES_DISABLE_GROUP_MANAGER_RECALL, "false"));
        } catch (Exception e) {

        }

        try {
            String strTypes = m_Server.getConfig().getProperty(BrokerConstants.MESSAGES_FORBIDDEN_CLIENT_SEND_TYPES);
            if(!StringUtil.isNullOrEmpty(strTypes)) {
                for (String strType:strTypes.split(",")) {
                    try {
                        int type = Integer.parseInt(strType);
                        mForbiddenClientSendTypes.add(type);
                    } catch (NumberFormatException e) {

                    }
                }
            }
        } catch (Exception e) {
        }

        try {
            String strTypes = m_Server.getConfig().getProperty(BrokerConstants.MESSAGES_BLACKLIST_EXCEPTION_TYPES);
            if(!StringUtil.isNullOrEmpty(strTypes)) {
                for (String strType:strTypes.split(",")) {
                    try {
                        int type = Integer.parseInt(strType);
                        mBlackListExceptionTypes.add(type);
                    } catch (NumberFormatException e) {

                    }
                }
            }
        } catch (Exception e) {
        }
        try {
            String strTypes = m_Server.getConfig().getProperty(BrokerConstants.MESSAGES_GROUP_MUTE_EXCEPTION_TYPES);
            if(!StringUtil.isNullOrEmpty(strTypes)) {
                for (String strType:strTypes.split(",")) {
                    try {
                        int type = Integer.parseInt(strType);
                        mGroupMuteExceptionTypes.add(type);
                    } catch (NumberFormatException e) {

                    }
                }
            }
        } catch (Exception e) {
        }
        try {
            String strTypes = m_Server.getConfig().getProperty(BrokerConstants.MESSAGES_GLOBAL_MUTE_EXCEPTION_TYPES);
            if(!StringUtil.isNullOrEmpty(strTypes)) {
                for (String strType:strTypes.split(",")) {
                    try {
                        int type = Integer.parseInt(strType);
                        mGlobalMuteExceptionTypes.add(type);
                    } catch (NumberFormatException e) {

                    }
                }
            }
        } catch (Exception e) {
        }

        try {
            mGroupInfoUpdateCallback = server.getConfig().getProperty(GROUP_INFO_UPDATE_CALLBACK);
        } catch (Exception e) {

        }

        try {
            mGroupMemberUpdateCallback = server.getConfig().getProperty(GROUP_MEMBER_UPDATE_CALLBACK);
        } catch (Exception e) {

        }

        try {
            mRelationUpdateCallback = server.getConfig().getProperty(RELATION_UPDATE_CALLBACK);
        } catch (Exception e) {

        }

        try {
            mUserInfoUpdateCallback = server.getConfig().getProperty(USER_INFO_UPDATE_CALLBACK);
        } catch (Exception e) {

        }

        try {
            mChannelInfoUpdateCallback = server.getConfig().getProperty(CHANNEL_INFO_UPDATE_CALLBACK);
        } catch (Exception e) {

        }

        try {
            mChatroomInfoUpdateCallback = server.getConfig().getProperty(CHATROOM_INFO_UPDATE_CALLBACK);
        } catch (Exception e) {

        }

        try {
            mChatroomMemberUpdateCallback = server.getConfig().getProperty(CHATROOM_MEMBER_UPDATE_CALLBACK);
        } catch (Exception e) {

        }

        try {
            mGroupAllowClientCustomOperationNotification = Boolean.parseBoolean(server.getConfig().getProperty(GROUP_Allow_Client_Custom_Operation_Notification));
        } catch (Exception e) {

        }

        try {
            mGroupVisibleQuitKickoffNotification = Integer.parseInt(server.getConfig().getProperty(GROUP_Visible_Quit_Kickoff_Notification, "0"));
        } catch (Exception e) {

        }

        try {
            mSyncDataPartSize = Integer.parseInt(server.getConfig().getProperty(SYNC_Data_Part_Size, "0"));
        } catch (Exception e) {

        }


        try {
            String userHideStr = server.getConfig().getProperty(USER_HIDE_PROPERTIES);
            if(!StringUtil.isNullOrEmpty(userHideStr)) {
                String[] proStrs = userHideStr.split(",");
                for (String proStr:proStrs) {
                    int value = Integer.parseInt(proStr);
                    if(value > 1 && value < 9) {
                        mUserHideProperties.add(value);
                    }
                }
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }


    }

    private void printMissConfigLog(String config, String defaultValue) {
        LOG.info("配置文件中缺少配置项目 {}, 缺省值为 {}，可以忽略本提醒，或者更新配置项", config, defaultValue);
    }

    @Override
    public void initStore() {
        updateSensitiveWord();
    }

    private Collection<WFCMessage.GroupMember> loadGroupMemberFromDB(HazelcastInstance hzInstance, String groupId) {
        Collection<WFCMessage.GroupMember> members = databaseStore.reloadGroupMemberFromDB(hzInstance, groupId);
        return members;
    }

    private void updateSensitiveWord() {
        long now = System.currentTimeMillis();
        if (now - lastUpdateSensitiveTime > 2 * 60 * 60 * 1000) {
            synchronized (this) {
                if (now - lastUpdateSensitiveTime > 2 * 60 * 60 * 1000) {
                    lastUpdateSensitiveTime = now;
                } else {
                    return;
                }
            }
            Set<String> sensitiveWords = databaseStore.getSensitiveWord();
            mSensitiveFilter = new SensitiveFilter(sensitiveWords);
        }
    }

    @Override
    public DatabaseStore getDatabaseStore() {
        return databaseStore;
    }

    @Override
    public WFCMessage.Message storeMessage(String fromUser, String fromClientId, WFCMessage.Message message) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<Long, MessageBundle> mIMap = hzInstance.getMap(MESSAGES_MAP);


        MessageBundle messageBundle = new MessageBundle(message.getMessageId(), fromUser, fromClientId, message);
        if (message.getContent().getPersistFlag() ==  Transparent) {
            mIMap.put(message.getMessageId(), messageBundle, 10, TimeUnit.SECONDS);
        } else {
            mIMap.put(message.getMessageId(), messageBundle, 7, TimeUnit.DAYS);
        }

        return message;
    }

    @Override
    public void storeSensitiveMessage(WFCMessage.Message message) {
        databaseStore.persistSensitiveMessage(message);
    }


    @Override
    public int getNotifyReceivers(String fromUser, WFCMessage.Message.Builder messageBuilder, Set<String> notifyReceivers) {
        WFCMessage.Message message = messageBuilder.build();
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        int type = message.getConversation().getType();
        int pullType = ProtoConstants.PullType.Pull_Normal;

        if (type == ProtoConstants.ConversationType.ConversationType_Private) {
            notifyReceivers.add(fromUser);
            notifyReceivers.add(message.getConversation().getTarget());
            pullType = ProtoConstants.PullType.Pull_Normal;
        } else if (type == ProtoConstants.ConversationType.ConversationType_Group) {
            notifyReceivers.add(fromUser);

            if (!StringUtil.isNullOrEmpty(message.getToUser())) {
                notifyReceivers.add(message.getToUser());
            } else if(message.getToList()!=null && !message.getToList().isEmpty()) {
                MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
                Collection<WFCMessage.GroupMember> members = groupMembers.get(message.getConversation().getTarget());
                if (members == null || members.size() == 0) {
                    members = loadGroupMemberFromDB(hzInstance, message.getConversation().getTarget());
                }
                for (WFCMessage.GroupMember member : members) {
                    if (member.getType() != GroupMemberType_Removed && message.getToList().contains(member.getMemberId())) {
                        notifyReceivers.add(member.getMemberId());
                    }
                }
            } else {
                MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
                Collection<WFCMessage.GroupMember> members = groupMembers.get(message.getConversation().getTarget());
                if (members == null || members.size() == 0) {
                    members = loadGroupMemberFromDB(hzInstance, message.getConversation().getTarget());
                }
                for (WFCMessage.GroupMember member : members) {
                    if (member.getType() != GroupMemberType_Removed) {
                        notifyReceivers.add(member.getMemberId());
                    }
                }
            }

            //如果是群助手的消息，返回pull type group，否则返回normal
            //群助手还没有实现
            pullType = ProtoConstants.PullType.Pull_Normal;
        } else if (type == ProtoConstants.ConversationType.ConversationType_ChatRoom) {
            boolean isDirect = !StringUtil.isNullOrEmpty(message.getToUser());
            Collection<UserClientEntry> entries = getChatroomMembers(message.getConversation().getTarget());
            for (UserClientEntry entry : entries) {
                if (isDirect) {
                    if (entry.userId.equals(message.getToUser())) {
                        notifyReceivers.add(message.getToUser());
                        break;
                    }
                } else {
                    notifyReceivers.add(entry.userId);
                }
            }

            pullType = ProtoConstants.PullType.Pull_ChatRoom;
        } else if(type == ProtoConstants.ConversationType.ConversationType_Channel) {
            WFCMessage.ChannelInfo channelInfo = getChannelInfo(message.getConversation().getTarget());
            if (channelInfo != null) {
                notifyReceivers.add(fromUser);
                if (channelInfo.getOwner().equals(fromUser)) {
                    if((channelInfo.getStatus() & ProtoConstants.ChannelState.Channel_State_Mask_Global) > 0) {
                        if((message.getToList() != null && !message.getToList().isEmpty())) {
                            notifyReceivers.addAll(message.getToList());
                        } else {
                            notifyReceivers.addAll(getAllEnds());
                        }
                    } else {
                        Collection<String> listeners = getChannelListener(message.getConversation().getTarget());
                        if (!StringUtil.isNullOrEmpty(message.getToUser())) {
                            if (listeners.contains(message.getToUser()) || (channelInfo.getStatus() & ProtoConstants.ChannelState.Channel_State_Mask_Message_Unsubscribed) > 0) {
                                notifyReceivers.add(message.getToUser());
                            }
                        } else if (message.getToList() != null && !message.getToList().isEmpty()) {
                            for (String to : message.getToList()) {
                                if (listeners.contains(to) || (channelInfo.getStatus() & ProtoConstants.ChannelState.Channel_State_Mask_Message_Unsubscribed) > 0) {
                                    notifyReceivers.add(to);
                                }
                            }
                        } else {
                            notifyReceivers.addAll(listeners);
                        }
                    }
                } else {
                    if (StringUtil.isNullOrEmpty(channelInfo.getCallback()) || channelInfo.getAutomatic() == 0) {
                        notifyReceivers.add(channelInfo.getOwner());
                    }
                }
            } else {
                LOG.error("Channel not exist");
            }
        }
//
//        if (message.getContent().getPersistFlag() == Transparent) {
//            notifyReceivers.remove(fromUser);
//        }

        return pullType;
    }

    @Override
    public Set<String> getAllEnds() {
        return databaseStore.getAllEnds();
    }
    @Override
    public WFCMessage.PullMessageResult fetchMessage(String user, String exceptClientId, long fromMessageId, int pullType) {
        WFCMessage.PullMessageResult.Builder builder = WFCMessage.PullMessageResult.newBuilder();

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<Long, MessageBundle> mIMap = hzInstance.getMap(MESSAGES_MAP);

        MemorySessionStore.Session session = m_Server.getStore().sessionsStore().getSession(exceptClientId);
        session.refreshLastActiveTime();
        if (pullType != ProtoConstants.PullType.Pull_ChatRoom) {
            session.setUnReceivedMsgs(0);
        } else {
            session.refreshLastChatroomActiveTime();
        }

        String chatroomId = null;
        if (pullType == ProtoConstants.PullType.Pull_ChatRoom) {
            chatroomId = (String)m_Server.getHazelcastInstance().getMap(USER_CHATROOM).get(user);
        }


        long head = fromMessageId;
        long current = fromMessageId;

        TreeMap<Long, Long> maps;

        if (pullType != ProtoConstants.PullType.Pull_ChatRoom) {
            maps = userMessages.get(user);
            if (maps == null) {
                loadUserMessages(user);
            }
            maps = userMessages.get(user);
        } else {
            maps = chatroomMessages.get(user);
            if (maps == null) {
                mWriteLock.lock();
                try {
                    maps = chatroomMessages.get(user);
                    if (maps == null) {
                        maps = new TreeMap<>();
                        chatroomMessages.put(user, maps);
                    }
                } finally {
                    mWriteLock.unlock();
                }
            }
        }

        mReadLock.lock();
        int size = 0;
        try {
            if (pullType != ProtoConstants.PullType.Pull_ChatRoom) {
                userMaxPullSeq.compute(user, (s, aLong) -> {
                    if (aLong == null || aLong < fromMessageId) {
                        return fromMessageId;
                    } else {
                        return aLong;
                    }
                });
            }

            boolean noRoaming = false;
            if (pullType == ProtoConstants.PullType.Pull_Normal && fromMessageId == 0) {
                if(!IS_MESSAGE_ROAMING) {
                    noRoaming = true;
                }
                session.setPullHistoryMsg(true);
            }

            while (true) {
                Map.Entry<Long, Long> entry = maps.higherEntry(current);
                if (entry == null) {
                    break;
                }
                current = entry.getKey();
                long targetMessageId = entry.getValue();

                MessageBundle bundle = mIMap.get(targetMessageId);

                if (bundle != null) {
                    if (exceptClientId == null || session.isPullHistoryMsg() || !exceptClientId.equals(bundle.getFromClientId()) || !user.equals(bundle.getFromUser())) {

                        if (pullType == ProtoConstants.PullType.Pull_ChatRoom) {
                            if (!bundle.getMessage().getConversation().getTarget().equals(chatroomId)) {
                                continue;
                            }
                        }

                        if (noRoaming && (System.currentTimeMillis() - bundle.getMessage().getServerTimestamp() > 5 * 60 * 1000)) {
                            continue;
                        }

                        if (bundle.getMessage().getContent().getExpireDuration() > 0) {
                            if (System.currentTimeMillis() > bundle.getMessage().getServerTimestamp() + bundle.getMessage().getContent().getExpireDuration()*1000) {
                                continue;
                            }
                        }

                        size += bundle.getMessage().getSerializedSize();
                        if (size >= 512 * 1024) { //3M
                            if(builder.getMessageCount() == 0){
                                builder.addMessage(bundle.getMessage());
                            }
                            break;
                        }
                        builder.addMessage(bundle.getMessage());
                    }
                }
            }

            Map.Entry<Long, Long> lastEntry = maps.lastEntry();
            if (lastEntry != null) {
                head = lastEntry.getKey();
            }
            if (pullType == ProtoConstants.PullType.Pull_Normal && current == head) {
                session.setPullHistoryMsg(false);
            }
        } finally {
            mReadLock.unlock();
        }

        builder.setCurrent(current);
        builder.setHead(head);
        return builder.build();
    }

    @Override
    public WFCMessage.PullMessageResult loadRemoteMessages(String user, WFCMessage.Conversation conversation, long beforeUid, int count) {
        WFCMessage.PullMessageResult.Builder builder = WFCMessage.PullMessageResult.newBuilder();
        List<WFCMessage.Message> messages;
        if (IS_MESSAGE_REMOTE_HISTORY_MESSAGE) {
           messages = databaseStore.loadRemoteMessages(user, conversation, beforeUid, count);
        } else {
           messages = new ArrayList<>();
        }
        builder.setCurrent(0).setHead(0);
        if(messages != null) {
            builder.addAllMessage(messages);
        }
        return builder.build();
    }

    @Override
    public Collection<UserClientEntry> getChatroomMembers(String chatroomId) {
        MultiMap<String, UserClientEntry> chatroomMembers = m_Server.getHazelcastInstance().getMultiMap(CHATROOM_MEMBER_IDS);
        Collection<UserClientEntry> members = chatroomMembers.get(chatroomId);
        return members;
    }
    //Todo chatroom
    @Override
    public WFCMessage.PullMessageResult fetchChatroomMessage(String fromUser, String chatroomId, String exceptClientId, long fromMessageId) {
        WFCMessage.PullMessageResult.Builder builder = WFCMessage.PullMessageResult.newBuilder();

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<Long, MessageBundle> mIMap = hzInstance.getMap(MESSAGES_MAP);


        long head = fromMessageId;
        long current = fromMessageId;

        TreeMap<Long, Long> maps = chatroomMessages.get(chatroomId);
        if (maps == null) {
            mWriteLock.lock();
            try {
                maps = chatroomMessages.get(chatroomId);
                if (maps == null) {
                    maps = new TreeMap<>();
                    chatroomMessages.put(chatroomId, maps);
                }
            } finally {
                mWriteLock.unlock();
            }
        }

        mReadLock.lock();
        int size = 0;
        try {
            maps = chatroomMessages.get(chatroomId);

            while (true) {
                Map.Entry<Long, Long> entry = maps.higherEntry(current);
                if (entry == null) {
                    break;
                }
                current = entry.getKey();
                long targetMessageId = entry.getValue();

                MessageBundle bundle = mIMap.get(targetMessageId);
                if (bundle != null) {
                    if (exceptClientId == null || !exceptClientId.equals(bundle.getFromClientId()) || !fromUser.equals(bundle.getFromUser())) {
                        if (bundle.getMessage().getContent().getExpireDuration() > 0) {
                            if (System.currentTimeMillis() > bundle.getMessage().getServerTimestamp() + bundle.getMessage().getContent().getExpireDuration() * 1000) {
                                continue;
                            }
                        }

                        size += bundle.getMessage().getSerializedSize();
                        if (size >= 3 * 1024 * 1024) { //3M
                            break;
                        }
                        builder.addMessage(bundle.getMessage());
                    }
                }
            }

            Map.Entry<Long, Long> lastEntry = maps.lastEntry();
            if (lastEntry != null) {
                head = lastEntry.getKey();
            }
        } finally {
            mReadLock.unlock();
        }

        builder.setCurrent(current);
        builder.setHead(head);
        return builder.build();
    }

    @Override
    public long insertUserMessages(String sender, int conversationType, String target, int line, int messageContentType, String user, long messageId) {
        // messageId是全局的，messageSeq是跟个人相关的，理论上messageId的增长数度远远大于seq。
        // 考虑到一种情况，当服务器发生变化，用户发生迁移后，messageSeq还需要保持有序。 要么把Seq持久化，要么在迁移后Seq取一个肯定比以前更大的数字（这个数字就是messageId）
        // 这里选择使用后面一种情况
        long messageSeq = 0;

        if (conversationType == ProtoConstants.ConversationType.ConversationType_ChatRoom) {
            return insertChatroomMessages(target, line, messageId);
        }


        mWriteLock.lock();
        try {
            TreeMap<Long, Long> maps = userMessages.get(user);
            if (maps == null) {
                maps = databaseStore.reloadUserMessageMaps(user);
                userMessages.put(user, maps);
            }

            Map.Entry<Long, Long> lastEntry = maps.lastEntry();
            if (lastEntry != null) {
                messageSeq = (lastEntry.getKey() + 1);
            }
            Long maxPullSeq = userMaxPullSeq.get(user);
            if (maxPullSeq != null && maxPullSeq > messageSeq) {
                messageSeq = maxPullSeq + 1;
            }

            if (messageSeq == 0) {
                messageSeq = messageId;
            }

            maps.put(messageSeq, messageId);
            if (maps.size() > MAX_MESSAGE_QUEUE) {
                maps.remove(maps.firstKey());
            }
        } finally {
            mWriteLock.unlock();
        }

        databaseStore.persistUserMessage(user, messageId, messageSeq);
        return messageSeq;
    }

    @Override
    public Collection<String> getChatroomMemberClient(String userId) {
        String chatroomId = (String)m_Server.getHazelcastInstance().getMap(USER_CHATROOM).get(userId);
        if (StringUtil.isNullOrEmpty(chatroomId)) {
            return new HashSet<>();
        }
        MultiMap<String, UserClientEntry> chatroomMembers = m_Server.getHazelcastInstance().getMultiMap(CHATROOM_MEMBER_IDS);
        if (chatroomMembers == null) {
            return new HashSet<>();
        }

        Collection<UserClientEntry> entries = chatroomMembers.get(chatroomId);

        HashSet<String> clientIds = new HashSet<>();
        if (entries == null) {
            return clientIds;
        }
        for (UserClientEntry entry : entries
             ) {
            clientIds.add(entry.clientId);
        }
        return clientIds;
    }

    @Override
    public boolean checkUserClientInChatroom(String user, String clientId, String chatroomId) {
        MemorySessionStore.Session session = m_Server.getStore().sessionsStore().getSession(clientId);

        String existChatroomId = (String)m_Server.getHazelcastInstance().getMap(USER_CHATROOM).get(user);
        if (chatroomId == null) {
            if (existChatroomId == null) {
                return false;
            } else {
                chatroomId = existChatroomId;
            }
        }

        if (StringUtil.isNullOrEmpty(existChatroomId) || !existChatroomId.equals(chatroomId)) {
            if (mChatroomRejoinWhenActive) {
                if (!StringUtil.isNullOrEmpty(existChatroomId)) {
                    handleQuitChatroom(user, clientId, existChatroomId);
                }
                handleJoinChatroom(user, clientId, chatroomId);
            } else {
                return false;
            }
        }

        if (!mChatroomRejoinWhenActive && !checkChatroomParticipantIdelTime(session)) {
            handleQuitChatroom(user, clientId, chatroomId);
            return false;
        }

        MultiMap<String, UserClientEntry> chatroomMembers = m_Server.getHazelcastInstance().getMultiMap(CHATROOM_MEMBER_IDS);
        if (chatroomMembers == null) {
            return false;
        }

        Collection<UserClientEntry> entries = chatroomMembers.get(existChatroomId);
        if (entries == null) {
            return false;
        }

        for (UserClientEntry entry : entries
            ) {
            if (entry.clientId.equals(clientId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long insertChatroomMessages(String target, int line, long messageId) {
        // messageId是全局的，messageSeq是跟个人相关的，理论上messageId的增长数度远远大于seq。
        // 考虑到一种情况，当服务器发生变化，用户发生迁移后，messageSeq还需要保持有序。 要么把Seq持久化，要么在迁移后Seq取一个肯定比以前更大的数字（这个数字就是messageId）
        // 这里选择使用后面一种情况
        long messageSeq = 0;

        mWriteLock.lock();
        try {
            TreeMap<Long, Long> maps = chatroomMessages.get(target);
            if (maps == null) {
                maps = new TreeMap<>();
                chatroomMessages.put(target, maps);
            }

            Map.Entry<Long, Long> lastEntry = maps.lastEntry();
            if (lastEntry != null) {
                messageSeq = (lastEntry.getKey() + 1);
            }


            if (messageSeq == 0) {
                messageSeq = messageId;
            }

            maps.put(messageSeq, messageId);
            if (maps.size() > MAX_CHATROOM_MESSAGE_QUEUE) {
                maps.remove(maps.firstKey());
            }
        } finally {
            mWriteLock.unlock();
        }

        return messageSeq;
    }

    @Override
    public WFCMessage.GroupInfo createGroup(String fromUser, WFCMessage.GroupInfo groupInfo, List<WFCMessage.GroupMember> memberList, String memberExtra, boolean isAdmin) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);

        String groupId = null;
        long dt = System.currentTimeMillis();
        if (StringUtil.isNullOrEmpty(groupInfo.getTargetId())) {
            groupId = getShortUUID();
        } else {
            groupId = groupInfo.getTargetId();
        }

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);

        String owner = fromUser;
        if (isAdmin && !StringUtil.isNullOrEmpty(groupInfo.getOwner())) {
            owner = groupInfo.getOwner();
        }

        List<WFCMessage.GroupMember> updatedMemberList = new ArrayList<>();
        boolean hasOwnerMember = false;
        for (WFCMessage.GroupMember member : memberList) {
            WFCMessage.GroupMember.Builder builder = member.toBuilder();
            builder.setUpdateDt(dt).setCreateDt(dt);
            if (member.getMemberId().equals(owner)) {
                builder.setType(ProtoConstants.GroupMemberType.GroupMemberType_Owner);
                hasOwnerMember = true;
            } else {
                if (isAdmin) {
                    if (member.getType() == GroupMemberType_Owner) {
                        builder.setType(GroupMemberType_Normal);
                        LOG.error("group member conflicted, group info owner is {}, and group member {} type is owner, set the member to normal member", owner, member.getMemberId());
                    }
                } else {
                    builder.setType(GroupMemberType_Normal);
                }
            }

            if(!StringUtil.isNullOrEmpty(memberExtra)) {
                builder.setExtra(memberExtra);
            }

            member = builder.build();
            groupMembers.put(groupId, member);
            updatedMemberList.add(member);
            dt++;
        }

        if(!hasOwnerMember && !StringUtil.isNullOrEmpty(owner)) {
            WFCMessage.GroupMember member;
            if(!StringUtil.isNullOrEmpty(memberExtra)) {
                member = WFCMessage.GroupMember.newBuilder().setMemberId(owner).setExtra(memberExtra).setUpdateDt(dt).setCreateDt(dt).setType(ProtoConstants.GroupMemberType.GroupMemberType_Owner).build();
            } else {
                member = WFCMessage.GroupMember.newBuilder().setMemberId(owner).setUpdateDt(dt).setCreateDt(dt).setType(ProtoConstants.GroupMemberType.GroupMemberType_Owner).build();
            }
            groupMembers.put(groupId, member);
            updatedMemberList.add(member);
        }

        groupInfo = groupInfo.toBuilder()
            .setTargetId(groupId)
            .setName(groupInfo.getName())
            .setPortrait(groupInfo.getPortrait())
            .setType(groupInfo.getType())
            .setExtra(groupInfo.getExtra())
            .setUpdateDt(dt)
            .setMemberUpdateDt(dt)
            .setMemberCount(updatedMemberList.size())
            .setOwner(owner)
            .build();

        mIMap.set(groupId, groupInfo);
        databaseStore.persistGroupMember(groupId, updatedMemberList, false);

        callbackGroupEvent(fromUser, groupInfo.getTargetId(), ProtoConstants.GroupUpdateEventType.Group_Event_Create);
        return groupInfo;
    }


    private void callbackGroupEvent(String operatorId, String groupId, int type) {
        if (!StringUtil.isNullOrEmpty(mGroupInfoUpdateCallback)) {
            GroupUpdateEvent event = new GroupUpdateEvent();
            event.operatorId = operatorId;
            event.groupId = groupId;
            event.type = type;
            m_Server.getCallbackScheduler().execute(() -> {
                try {
                    HttpUtils.httpJsonPost(mGroupInfoUpdateCallback, new Gson().toJson(event));
                } catch (Exception e) {
                    e.printStackTrace();
                    Utility.printExecption(LOG, e, EVENT_CALLBACK_Exception);
                }
            });
        }
    }

    private void callbackGroupMemberEvent(String operatorId, String groupId, List<String> memberIds, int type, String value) {
        if (!StringUtil.isNullOrEmpty(mGroupMemberUpdateCallback)) {
            GroupMemberUpdateEvent event = new GroupMemberUpdateEvent();
            event.operatorId = operatorId;
            event.groupId = groupId;
            event.memberIds = memberIds;
            event.type = type;
            event.value = value;
            m_Server.getCallbackScheduler().execute(() -> {
                try {
                    HttpUtils.httpJsonPost(mGroupMemberUpdateCallback, new Gson().toJson(event));
                } catch (Exception e) {
                    e.printStackTrace();
                    Utility.printExecption(LOG, e, EVENT_CALLBACK_Exception);
                }
            });
        }
    }

    private void callbackRelationEvent(String userId, String targetId, int type, String value) {
        if (!StringUtil.isNullOrEmpty(mRelationUpdateCallback)) {
            RelationUpdateEvent event = new RelationUpdateEvent();
            event.userId = userId;
            event.targetId = targetId;
            event.type = type;
            event.value = value;
            m_Server.getCallbackScheduler().execute(() -> {
                try {
                    HttpUtils.httpJsonPost(mRelationUpdateCallback, new Gson().toJson(event));
                } catch (Exception e) {
                    e.printStackTrace();
                    Utility.printExecption(LOG, e, EVENT_CALLBACK_Exception);
                }
            });
        }
    }

    private void callbackUserInfoEvent(WFCMessage.User user) {
        if (!StringUtil.isNullOrEmpty(mUserInfoUpdateCallback)) {
            m_Server.getCallbackScheduler().execute(() -> {
                try {
                    HttpUtils.httpJsonPost(mUserInfoUpdateCallback, new Gson().toJson(InputOutputUserInfo.fromPbUser(user)));
                } catch (Exception e) {
                    e.printStackTrace();
                    Utility.printExecption(LOG, e, EVENT_CALLBACK_Exception);
                }
            });
        }
    }

    private void callbackChannelInfoUpdateEvent(String operatorId, String channelId, int type) {
        if (!StringUtil.isNullOrEmpty(mChannelInfoUpdateCallback)) {
            ChannelUpdateEvent event = new ChannelUpdateEvent();
            event.operatorId = operatorId;
            event.channelId = channelId;
            event.type = type;
            m_Server.getCallbackScheduler().execute(() -> {
                try {
                    HttpUtils.httpJsonPost(mChannelInfoUpdateCallback, new Gson().toJson(event));
                } catch (Exception e) {
                    e.printStackTrace();
                    Utility.printExecption(LOG, e, EVENT_CALLBACK_Exception);
                }
            });
        }
    }

    private void callbackChatroomInfoUpdateEvent(String chatroomId, int type) {
        if (!StringUtil.isNullOrEmpty(mChatroomInfoUpdateCallback)) {
            ChatroomUpdateEvent event = new ChatroomUpdateEvent();
            event.chatroomId = chatroomId;
            event.type = type;
            m_Server.getCallbackScheduler().execute(() -> {
                try {
                    HttpUtils.httpJsonPost(mChatroomInfoUpdateCallback, new Gson().toJson(event));
                } catch (Exception e) {
                    e.printStackTrace();
                    Utility.printExecption(LOG, e, EVENT_CALLBACK_Exception);
                }
            });
        }
    }

    private void callbackChatroomMemberEvent(String operatorId, String chatroomId, List<String> memberIds, int type) {
        if (!StringUtil.isNullOrEmpty(mChatroomMemberUpdateCallback)) {
            ChatroomMemberUpdateEvent event = new ChatroomMemberUpdateEvent();
            event.operatorId = operatorId;
            event.chatroomId = chatroomId;
            event.memberIds = memberIds;
            event.type = type;
            m_Server.getCallbackScheduler().execute(() -> {
                try {
                    HttpUtils.httpJsonPost(mChatroomMemberUpdateCallback, new Gson().toJson(event));
                } catch (Exception e) {
                    e.printStackTrace();
                    Utility.printExecption(LOG, e, EVENT_CALLBACK_Exception);
                }
            });
        }
    }

    @Override
    public ErrorCode addGroupMembers(String operator, boolean isAdmin, String groupId, List<WFCMessage.GroupMember> memberList, String extra) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);

        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);
        if (groupInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }


        int maxCount = Integer.MAX_VALUE;

        if (!isAdmin) {
            boolean isMember = false;
            boolean isManager = false;
            WFCMessage.GroupMember gm = getGroupMember(groupId, operator);
            if (gm != null) {
                if (gm.getType() == GroupMemberType_Removed && !(memberList.size() == 1 && operator.equals(memberList.get(0).getMemberId()))) {
                    return ErrorCode.ERROR_CODE_NOT_IN_GROUP;
                } else if (gm.getType() == GroupMemberType_Silent) {
                    return ErrorCode.ERROR_CODE_NOT_RIGHT;
                }
                isManager = (gm.getType() == GroupMemberType_Manager || gm.getType() == GroupMemberType_Owner);

                if (gm.getType() != GroupMemberType_Removed) {
                    isMember = true;
                }
            }

            if (groupInfo.getType() == ProtoConstants.GroupType.GroupType_Restricted && ((groupInfo.getOwner() == null || !groupInfo.getOwner().equals(operator)) && !isManager)) {
                if (groupInfo.getJoinType() == 2) {
                    return ErrorCode.ERROR_CODE_NOT_RIGHT;
                } else if (groupInfo.getJoinType() == 1) {
                    if (memberList.size() == 1 && operator.equals(memberList.get(0).getMemberId()))
                    return ErrorCode.ERROR_CODE_NOT_RIGHT;
                }
            }

            if (!isMember && !(memberList.size() == 1 && operator.equals(memberList.get(0).getMemberId()))) {
                return ErrorCode.ERROR_CODE_NOT_IN_GROUP;
            }

            SystemSettingPojo maxMemberSetting = databaseStore.getSystemSetting(ProtoConstants.SystemSettingType.Group_Max_Member_Count);
            if (maxMemberSetting != null) {
                try {
                    maxCount = Integer.parseInt(maxMemberSetting.value);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
        }

        long updateDt = System.currentTimeMillis();

        List<WFCMessage.GroupMember> tmp = new ArrayList<>();
        ArrayList<String> newInviteUsers = new ArrayList<>();
        for (WFCMessage.GroupMember member : memberList) {
            WFCMessage.GroupMember.Builder builder = member.toBuilder();
            builder.setUpdateDt(updateDt).setCreateDt(updateDt);
            if (member.getMemberId().equals(groupInfo.getOwner())) {
                builder.setType(GroupMemberType_Owner).build();
            } else {
                if (isAdmin) {
                    if (member.getType() == GroupMemberType_Owner) {
                        builder.setType(GroupMemberType_Normal);
                        LOG.error("group member conflicted, group info owner is {}, and group member {} type is owner, set the member to normal member", groupInfo.getOwner(), member.getMemberId());
                    }
                } else {
                    builder.setType(GroupMemberType_Normal);
                }
            }
            if(!StringUtil.isNullOrEmpty(extra)) {
                builder.setExtra(extra);
            }
            member = builder.build();
            tmp.add(member);
            newInviteUsers.add(member.getMemberId());
            updateDt++;
        }
        memberList = tmp;

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }

        if (maxCount != Integer.MAX_VALUE) {
            int existCount = 0;
            for (WFCMessage.GroupMember member : members) {
                if (member.getType() != GroupMemberType_Removed) {
                    existCount++;
                }
            }
            if (existCount + newInviteUsers.size() > maxCount) {
                return ErrorCode.ERROR_CODE_GROUP_EXCEED_MAX_MEMBER_COUNT;
            }
        }

        databaseStore.persistGroupMember(groupId, memberList, true);
        databaseStore.updateGroupMemberCountDt(groupId);

        groupMembers.remove(groupId);
        mIMap.evict(groupId);

        List<String> memberIds = new ArrayList<>();
        for (WFCMessage.GroupMember member : memberList) {
            if (member.getType() != GroupMemberType_Removed) {
                memberIds.add(member.getMemberId());
            }
        }

        callbackGroupMemberEvent(operator, groupId, memberIds, ProtoConstants.GroupMemberUpdateEventType.Group_Member_Event_Join, null);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode kickoffGroupMembers(String operator, boolean isAdmin, String groupId, List<String> memberList) {
        removeGroupMember(groupId, memberList);
        removeFavGroup(groupId, memberList);
        removeGroupUserSettings(groupId, memberList);

        callbackGroupMemberEvent(operator, groupId, memberList, ProtoConstants.GroupMemberUpdateEventType.Group_Member_Event_Kickoff, null);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    void removeGroupMember(String groupId, List<String> memberIds) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        databaseStore.removeGroupMember(groupId, memberIds);
        databaseStore.updateGroupMemberCountDt(groupId);

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
        groupMembers.remove(groupId);
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);
        mIMap.evict(groupId);
    }

    void removeFavGroup(String groupId, List<String> memberIds) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);

        databaseStore.removeFavGroup(groupId, memberIds);

        for (String member : memberIds) {
            userSettingMap.remove(member);
        }

    }

    @Override
    public ErrorCode quitGroup(String operator, String groupId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);

        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);
        if (groupInfo == null) {
            MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
            Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
            if (members == null || members.size() == 0) {
                members = loadGroupMemberFromDB(hzInstance, groupId);
            }
            for (WFCMessage.GroupMember member :members) {
                if (member.getMemberId().equals(operator)) {
                    groupMembers.remove(groupId, member);
                    break;
                }
            }
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (groupInfo.getType() != ProtoConstants.GroupType.GroupType_Free && groupInfo.getOwner() != null && groupInfo.getOwner().equals(operator)) {
            MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
            Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
            if (members == null || members.size() == 0) {
                members = loadGroupMemberFromDB(hzInstance, groupId);
            }

            WFCMessage.GroupMember newOwner = null;
            for (WFCMessage.GroupMember member :members) {
                if (!member.getMemberId().equals(operator) && member.getType() != GroupMemberType_Removed) {
                    newOwner = member;
                    break;
                }
            }

            if (newOwner == null) {
                 return dismissGroup(operator, groupId, false);
            }

            transferGroup(operator, groupId, newOwner.getMemberId(), false);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        removeGroupMember(groupId, Arrays.asList(operator));
        removeFavGroup(groupId, Arrays.asList(operator));
        removeGroupUserSettings(groupId, Arrays.asList(operator));

        callbackGroupMemberEvent(operator, groupId, Arrays.asList(operator), ProtoConstants.GroupMemberUpdateEventType.Group_Member_Event_Leave, null);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public void clearUserGroups(String userId) {
        Set<String> groupIds = databaseStore.getUserGroupIds(userId);
        int[] removedCount = new int[1];
        for (String groupId : groupIds) {
            removedCount[0] = 0;
            ErrorCode errorCode = quitGroup(userId, groupId);
            LOG.info("clear user {} group {} result {}", userId, groupId, errorCode);
        }
    }


    private void removeGroupUserSettings(String groupId, List<String> users) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);

        databaseStore.removeGroupUserSettings(groupId, users);
        for (String userId:users) {
            userSettingMap.remove(userId);
        }

    }

    @Override
    public ErrorCode dismissGroup(String operator, String groupId, boolean isAdmin) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);


        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);
        if (groupInfo == null) {
            MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
            groupMembers.remove(groupId);

            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (!isAdmin && (groupInfo.getType() == ProtoConstants.GroupType.GroupType_Free ||
            (groupInfo.getType() == ProtoConstants.GroupType.GroupType_Restricted || groupInfo.getType() == ProtoConstants.GroupType.GroupType_Normal)
                && (groupInfo.getOwner() == null || !groupInfo.getOwner().equals(operator)))) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);

        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }

        groupMembers.remove(groupId);
        mIMap.remove(groupId);

        databaseStore.removeGroupMemberFromDB(groupId);

        ArrayList<String> ids = new ArrayList<>();
        if (members != null) {
            for (WFCMessage.GroupMember member : members) {
                ids.add(member.getMemberId());
            }
        }
        removeFavGroup(groupId, ids);
        removeGroupUserSettings(groupId, ids);

        callbackGroupEvent(operator, groupId, ProtoConstants.GroupUpdateEventType.Group_Event_Destroy);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode modifyGroupInfo(String operator, String groupId, int modifyType, String value, boolean isAdmin) {

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);

        WFCMessage.GroupInfo oldInfo = mIMap.get(groupId);

        if (oldInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (!isAdmin) {
            if (oldInfo.getType() == ProtoConstants.GroupType.GroupType_Restricted) {
                boolean isAllow = false;
                if ((oldInfo.getOwner() != null && oldInfo.getOwner().equals(operator))) {
                    isAllow = true;
                } else {
                    WFCMessage.GroupMember gm = getGroupMember(groupId, operator);
                    if (gm != null && gm.getType() == GroupMemberType_Manager) {
                        isAllow = true;
                    }
                }
                if (!isAllow) {
                    return ErrorCode.ERROR_CODE_NOT_RIGHT;
                }
            }

            if (oldInfo.getType() == ProtoConstants.GroupType.GroupType_Normal) {
                if (!operator.equals(oldInfo.getOwner())) {
                    WFCMessage.GroupMember gm = getGroupMember(groupId, operator);
                    boolean isManager = false;
                    if (gm != null && gm.getType() == GroupMemberType_Manager) {
                        isManager = true;
                    }
                    if (!isManager && modifyType != Modify_Group_Name && modifyType != Modify_Group_Portrait && modifyType != Modify_Group_Extra) {
                        return ErrorCode.ERROR_CODE_NOT_RIGHT;
                    }
                }
            }

            if (oldInfo.getType() == ProtoConstants.GroupType.GroupType_Free) {
                if (modifyType != Modify_Group_Name && modifyType != Modify_Group_Portrait && modifyType != Modify_Group_Extra) {
                    return ErrorCode.ERROR_CODE_NOT_RIGHT;
                }
            }
        }

        WFCMessage.GroupInfo.Builder newInfoBuilder = oldInfo.toBuilder();

        if (modifyType == Modify_Group_Name)
            newInfoBuilder.setName(value);
        else if(modifyType == Modify_Group_Portrait)
            newInfoBuilder.setPortrait(value);
        else if(modifyType == Modify_Group_Extra)
            newInfoBuilder.setExtra(value);
        else if(modifyType == Modify_Group_Mute)
            newInfoBuilder.setMute(Integer.parseInt(value));
        else if(modifyType == Modify_Group_JoinType)
            newInfoBuilder.setJoinType(Integer.parseInt(value));
        else if(modifyType == Modify_Group_PrivateChat)
            newInfoBuilder.setPrivateChat(Integer.parseInt(value));
        else if(modifyType == Modify_Group_Searchable)
            newInfoBuilder.setSearchable(Integer.parseInt(value));
        else
            return ErrorCode.INVALID_PARAMETER;


        newInfoBuilder.setUpdateDt(System.currentTimeMillis());
        databaseStore.persistGroupInfo(newInfoBuilder.build());
        mIMap.evict(groupId);

        if (modifyType == Modify_Group_Mute) {
            if (newInfoBuilder.getMute() > 0) {
                callbackGroupEvent(operator, groupId, ProtoConstants.GroupUpdateEventType.Group_Event_Mute);
            } else {
                callbackGroupEvent(operator, groupId, ProtoConstants.GroupUpdateEventType.Group_Event_Unmute);
            }
        } else {
            callbackGroupEvent(operator, groupId, ProtoConstants.GroupUpdateEventType.Group_Event_Update);
        }
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode modifyGroupAlias(String operator, String groupId, String alias, String memberId, boolean isAdmin) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();

        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);
        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);
        if (groupInfo == null) {
            groupInfo = databaseStore.getPersistGroupInfo(groupId);
            if (groupInfo != null) {
                mIMap.set(groupId, groupInfo);
            } else {
                return ErrorCode.ERROR_CODE_NOT_EXIST;
            }
        }

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }

        if (StringUtil.isNullOrEmpty(memberId)) {
            memberId = operator;
        } else {
            if (!isAdmin && !operator.equals(groupInfo.getOwner())) {
                WFCMessage.GroupMember operatorMember = null;
                WFCMessage.GroupMember targetMember = null;
                for (WFCMessage.GroupMember member : members) {
                    if (member.getMemberId().equals(operator)) {
                        operatorMember = member;
                        if (targetMember != null) {
                            break;
                        }
                    }
                    if ((member.getMemberId().equals(memberId))) {
                        targetMember = member;
                        if (operatorMember != null) {
                            break;
                        }
                    }
                }

                if (operatorMember == null) {
                    LOG.error("Modify group member alias error, the operator {} is not in group", operator);
                    return ErrorCode.ERROR_CODE_NOT_IN_GROUP;
                }

                if (targetMember == null) {
                    LOG.error("Modify group member alias error, the member {} is not in group", memberId);
                    return ErrorCode.ERROR_CODE_NOT_IN_GROUP;
                }

                if (operatorMember.getType() == GroupMemberType_Manager && targetMember.getType() != GroupMemberType_Manager) {
                    LOG.error("Modify group member alias error, the operator {} type is {} and the member {} type is {}", operator, operatorMember.getType(), memberId, targetMember.getType());
                    return ErrorCode.ERROR_CODE_NOT_RIGHT;
                }
            }
        }

        long updateDt = System.currentTimeMillis();

        boolean inGroup = false;
        for (WFCMessage.GroupMember member : members) {
            if (member.getMemberId().equals(memberId)) {
                groupMembers.remove(groupId, member);
                member = member.toBuilder().setAlias(alias).setUpdateDt(updateDt).build();
                databaseStore.persistGroupMember(groupId, Arrays.asList(member), false);
                databaseStore.updateGroupMemberDt(groupId, updateDt);
                groupMembers.put(groupId, member);

                mIMap.set(groupId, groupInfo.toBuilder().setUpdateDt(updateDt).setMemberUpdateDt(updateDt).build());
                inGroup = true;
                break;
            }
        }

        if (!inGroup) {
            LOG.error("Modify group member alias error, the member {} is not in group", memberId);
            return ErrorCode.ERROR_CODE_NOT_IN_GROUP;
        }

        callbackGroupMemberEvent(operator, groupId, Arrays.asList(memberId), ProtoConstants.GroupMemberUpdateEventType.Group_Member_Event_Alias, alias);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public List<WFCMessage.GroupInfo> getGroupInfos(List<WFCMessage.UserRequest> requests) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);
        ArrayList<WFCMessage.GroupInfo> out = new ArrayList<>();
        for (WFCMessage.UserRequest request : requests) {
            WFCMessage.GroupInfo groupInfo = mIMap.get(request.getUid());
            if (groupInfo == null) {
                groupInfo = databaseStore.getPersistGroupInfo(request.getUid());
                if (groupInfo != null) {
                    mIMap.set(request.getUid(), groupInfo);
                }
            }

            if (groupInfo != null && groupInfo.getUpdateDt() > request.getUpdateDt()) {
                out.add(groupInfo);
            }
        }

        return out;
    }

    @Override
    public WFCMessage.GroupInfo getGroupInfo(String groupId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);

        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);
        return groupInfo;
    }

    @Override
    public Set<String> getUserGroupIds(String userId) {
        return databaseStore.getUserGroupIds(userId);
    }

    @Override
    public ErrorCode getGroupMembers(String fromUser, String groupId, long maxDt, List<WFCMessage.GroupMember> members) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);

        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);
        if (groupInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
        Collection<WFCMessage.GroupMember> memberCollection = groupMembers.get(groupId);
        if (memberCollection == null || memberCollection.size() == 0) {
            memberCollection = loadGroupMemberFromDB(hzInstance, groupId);
        }
        boolean notInGroup = true;
        WFCMessage.GroupMember self = null;
        for (WFCMessage.GroupMember member:memberCollection) {
            if (member.getUpdateDt() > maxDt) {
                members.add(member);
            }
            if (fromUser != null && notInGroup) {
                if (member.getMemberId().equals(fromUser)) {
                    if (member.getType() != GroupMemberType_Removed) {
                        notInGroup = false;
                    } else {
                        self = member;
                        break;
                    }
                }
            }
        }

        //server api fromUser is null
        if (fromUser != null && notInGroup) {
            members.clear();
            if (self != null) {
                members.add(self.toBuilder().setUpdateDt(0).setType(GroupMemberType_Removed).build());
            }
        }

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public WFCMessage.GroupMember getGroupMember(String groupId, String memberId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);

        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }

        if (members != null) {
            for (WFCMessage.GroupMember gm : members) {
                if (gm.getMemberId().equals(memberId)) {
                    return gm;
                }
            }
        }

        return null;
    }


    @Override
    public ErrorCode transferGroup(String operator, String groupId, String newOwner, boolean isAdmin) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);


        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);

        if (groupInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (!isAdmin && (groupInfo.getType() == ProtoConstants.GroupType.GroupType_Restricted || groupInfo.getType() == ProtoConstants.GroupType.GroupType_Normal)
            && (groupInfo.getOwner() == null || !groupInfo.getOwner().equals(operator))) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }

        //check the new owner is in member list? is that necessary?
        long updateDt = System.currentTimeMillis();
        groupInfo = groupInfo.toBuilder().setOwner(newOwner).setUpdateDt(updateDt).setMemberUpdateDt(updateDt).build();
        mIMap.set(groupId, groupInfo);

        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }
        int modifyMemeberCount = 0;
        for (WFCMessage.GroupMember member : members) {
            if (modifyMemeberCount == 2) {
                break;
            }
            if (newOwner.equals(member.getMemberId())) {
                groupMembers.remove(groupId, member);
                member = member.toBuilder().setType(GroupMemberType_Owner).setUpdateDt(updateDt).build();
                databaseStore.persistGroupMember(groupId, Arrays.asList(member), false);
                groupMembers.put(groupId, member);
                modifyMemeberCount++;
            } else if(member.getType() == GroupMemberType_Owner) {
                groupMembers.remove(groupId, member);
                member = member.toBuilder().setType(GroupMemberType_Normal).setUpdateDt(updateDt).build();
                databaseStore.persistGroupMember(groupId, Arrays.asList(member), false);
                groupMembers.put(groupId, member);
                modifyMemeberCount++;
            }
        }

        callbackGroupEvent(operator, groupId, ProtoConstants.GroupUpdateEventType.Group_Event_Transfer);

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode setGroupManager(String operator, String groupId, int type, List<String> userList, boolean isAdmin) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.GroupInfo> mIMap = hzInstance.getMap(GROUPS_MAP);


        WFCMessage.GroupInfo groupInfo = mIMap.get(groupId);

        if (groupInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (!isAdmin && (groupInfo.getType() == ProtoConstants.GroupType.GroupType_Restricted || groupInfo.getType() == ProtoConstants.GroupType.GroupType_Normal)
            && (groupInfo.getOwner() == null || !groupInfo.getOwner().equals(operator))) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }

        long updateDt = System.currentTimeMillis();
        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }
        for (WFCMessage.GroupMember member : members) {
            if (userList.contains(member.getMemberId())) {
                groupMembers.remove(groupId, member);
                member = member.toBuilder().setType(type == 0 ? ProtoConstants.GroupMemberType.GroupMemberType_Normal : ProtoConstants.GroupMemberType.GroupMemberType_Manager).setUpdateDt(updateDt).build();
                databaseStore.persistGroupMember(groupId, Arrays.asList(member), false);
                groupMembers.put(groupId, member);
            }
        }
        databaseStore.persistGroupInfo(groupInfo.toBuilder().setUpdateDt(updateDt).setMemberUpdateDt(updateDt).build());
        mIMap.evict(groupId);

        callbackGroupMemberEvent(operator, groupId, userList, ProtoConstants.GroupMemberUpdateEventType.Group_Member_Event_Type_Update, (type == 0 ? ProtoConstants.GroupMemberType.GroupMemberType_Normal : ProtoConstants.GroupMemberType.GroupMemberType_Manager) + "");
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public boolean isMemberInGroup(String memberId, String groupId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);

        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }
        for (WFCMessage.GroupMember member : members
            ) {
            if (member.getMemberId().equals(memberId) && member.getType() != GroupMemberType_Removed)
                return true;
        }

        return false;
    }

    @Override
    public ErrorCode canSendMessageInGroup(String memberId, String groupId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
        IMap<String, WFCMessage.GroupInfo> groups = hzInstance.getMap(GROUPS_MAP);
        WFCMessage.GroupInfo groupInfo = groups.get(groupId);
        boolean isMute = false;
        if (groupInfo != null) {
            if (groupInfo.getOwner().equals(memberId)) {
                return ErrorCode.ERROR_CODE_SUCCESS;
            }
            isMute = groupInfo.getMute()>0;
        }

        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }

        boolean isInGroup = false;
        for (WFCMessage.GroupMember member : members) {
            if (member.getMemberId().equals(memberId)) {
                if (member.getType() == GroupMemberType_Silent) {
                    return ErrorCode.ERROR_CODE_NOT_RIGHT;
                }

                if (isMute && member.getType() != GroupMemberType_Manager && member.getType() != GroupMemberType_Owner) {
                    return ErrorCode.ERROR_CODE_GROUP_MUTED;
                }

                if (member.getMemberId().equals(memberId) && member.getType() == GroupMemberType_Removed) {
                    return ErrorCode.ERROR_CODE_NOT_IN_GROUP;
                }
                isInGroup = true;
                break;
            }

        }

        if (!isInGroup) {
            return ErrorCode.ERROR_CODE_NOT_IN_GROUP;
        }

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public Set<String> getGroupManagers(String groupId, boolean includeOwner) {
        Set<String> ret = new HashSet<>();
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);

        Collection<WFCMessage.GroupMember> members = groupMembers.get(groupId);
        if (members == null || members.size() == 0) {
            members = loadGroupMemberFromDB(hzInstance, groupId);
        }
        for (WFCMessage.GroupMember member : members) {
            if(member.getType() == GroupMemberType_Manager || (member.getType() == GroupMemberType_Owner && includeOwner)) {
                ret.add(member.getMemberId());
            }
        }

        return ret;
    }

    @Override
    public ErrorCode recallMessage(long messageUid, String operatorId, String clientId, boolean isAdmin) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<Long, MessageBundle> mIMap = hzInstance.getMap(MESSAGES_MAP);

        MessageBundle messageBundle = mIMap.get(messageUid);
        long now = System.currentTimeMillis();
        if (messageBundle != null) {
            WFCMessage.Message message = messageBundle.getMessage();
            boolean canRecall = false;
            if (isAdmin) {
                canRecall = true;
            }
            if (!isAdmin && message.getFromUser().equals(operatorId)) {
                if (now - message.getServerTimestamp() > mRecallTimeLimit * 1000) {
                    return ErrorCode.ERROR_CODE_RECALL_TIME_EXPIRED;
                } else {
                    canRecall = true;
                }
            }

            if (!canRecall && !mDisableGroupManagerRecall && message.getConversation().getType() == ProtoConstants.ConversationType.ConversationType_Group) {
                IMap<String, WFCMessage.GroupInfo> groupMap = hzInstance.getMap(GROUPS_MAP);

                WFCMessage.GroupInfo groupInfo = groupMap.get(message.getConversation().getTarget());
                if (groupInfo == null) {
                    return ErrorCode.ERROR_CODE_RECALL_TIME_EXPIRED;
                }
                if (operatorId.equals(groupInfo.getOwner())) {
                    canRecall = true;
                }

                MultiMap<String, WFCMessage.GroupMember> groupMembers = hzInstance.getMultiMap(GROUP_MEMBERS);
                Collection<WFCMessage.GroupMember> members = groupMembers.get(message.getConversation().getTarget());
                if (members == null || members.size() == 0) {
                    members = loadGroupMemberFromDB(hzInstance, message.getConversation().getTarget());
                }
                for (WFCMessage.GroupMember member : members) {
                    if (member.getMemberId().equals(operatorId)) {
                        if (member.getType() == GroupMemberType_Manager || member.getType() == ProtoConstants.GroupMemberType.GroupMemberType_Owner) {
                            canRecall = true;
                        }
                        break;
                    }
                }
            }

            if (!canRecall && message.getConversation().getType() == ProtoConstants.ConversationType.ConversationType_ChatRoom) {
                IMap<String, WFCMessage.ChatroomInfo> chatroomInfoMap = hzInstance.getMap(CHATROOMS);
                WFCMessage.ChatroomInfo room = chatroomInfoMap.get(message.getConversation().getTarget());
                //todo check is manager
            }

            if (!canRecall) {
                return ErrorCode.ERROR_CODE_NOT_RIGHT;
            }

            JSONObject json = new JSONObject();
            json.put("s", message.getFromUser());
            json.put("ts", message.getServerTimestamp());
            json.put("t", message.getContent().getType());
            json.put("sc", message.getContent().getSearchableContent());
            json.put("c", message.getContent().getContent());
            json.put("e", message.getContent().getExtra());
            if (message.getContent().getData() != null && message.getContent().getData().size() > 0) {
                json.put("b", Base64.getEncoder().encode(message.getContent().getData().toByteArray()));
            }
            if (message.getContent().getMediaType() > 0) {
                json.put("mt", message.getContent().getMediaType());
            }
            if (!StringUtil.isNullOrEmpty(message.getContent().getRemoteMediaUrl())) {
                json.put("mu", message.getContent().getRemoteMediaUrl());
            }

            String recalledContent = json.toJSONString();

            message = message.toBuilder().setContent(WFCMessage.MessageContent.newBuilder().setContent(operatorId).setType(80).setData(ByteString.copyFrom(String.valueOf(messageUid).getBytes())).setExtra(recalledContent)).build();
            messageBundle.setMessage(message);
            messageBundle.setFromClientId(clientId);

            databaseStore.deleteMessage(messageUid);

            mIMap.put(messageUid, messageBundle, 7, TimeUnit.DAYS);
            return ErrorCode.ERROR_CODE_SUCCESS;
        } else {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }
    }

    public ErrorCode recallCastMessage(long messageUid, String operatorId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<Long, MessageBundle> mIMap = hzInstance.getMap(MESSAGES_MAP);

        MessageBundle messageBundle = mIMap.get(messageUid);
        if (messageBundle != null) {
            WFCMessage.Message message = messageBundle.getMessage();

            message = message.toBuilder().setContent(WFCMessage.MessageContent.newBuilder().setContent(operatorId).setType(80).setData(ByteString.copyFrom(String.valueOf(messageUid).getBytes()))).build();
            messageBundle.setMessage(message);
            messageBundle.setFromClientId(null);

            databaseStore.deleteMessage(messageUid);
            mIMap.set(messageUid, messageBundle, 7, TimeUnit.DAYS);
            return ErrorCode.ERROR_CODE_SUCCESS;
        } else {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }
    }

    @Override
    public void clearUserMessages(String userId) {
        mWriteLock.lock();
        try {
            userMessages.remove(userId);
        } finally {
            mWriteLock.unlock();
        }

        databaseStore.clearUserMessage(userId);
    }

    @Override
    public WFCMessage.Robot getRobot(String robotId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.Robot> mRobotMap = hzInstance.getMap(ROBOTS);
        return mRobotMap.get(robotId);
    }

    @Override
    public void addRobot(WFCMessage.Robot robot) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.Robot> mUserMap = hzInstance.getMap(ROBOTS);
        mUserMap.put(robot.getUid(), robot);
    }


    @Override
    public ErrorCode getUserInfo(String fromUser, List<WFCMessage.UserRequest> requestList, WFCMessage.PullUserResult.Builder builder) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);

        for (WFCMessage.UserRequest request : requestList
            ) {
            WFCMessage.User user = mUserMap.get(request.getUid());
            if (user == null) {
                user = databaseStore.getPersistUser(request.getUid());
                if (user != null) {
                    mUserMap.set(request.getUid(), user);
                }
            }
            WFCMessage.UserResult.Builder resultBuilder = WFCMessage.UserResult.newBuilder();
            if (user == null) {
                LOG.warn("Get user info, user {} not exist", request.getUid());
                user = WFCMessage.User.newBuilder().setUid(request.getUid()).build();
                resultBuilder.setUser(user);
                resultBuilder.setCode(ProtoConstants.UserResultCode.NotFound);
            } else {
                LOG.debug("Get user info, user {}  userDt {} : request {}",request.getUid(), user.getUpdateDt(), request.getUpdateDt());
                if (user.getUpdateDt() > request.getUpdateDt()) {
                    if(!mUserHideProperties.isEmpty() && !user.getUid().equals(fromUser)) {
                        WFCMessage.User.Builder userBuilder = user.toBuilder();
                        for (Integer i:mUserHideProperties) {
                            if(i == Modify_Gender) {
                                userBuilder.clearGender();
                            } else if(i == Modify_Mobile) {
                                userBuilder.clearMobile();
                            } else if(i == Modify_Email) {
                                userBuilder.clearEmail();
                            } else if(i == Modify_Address) {
                                userBuilder.clearAddress();
                            } else if(i == Modify_Company) {
                                userBuilder.clearCompany();
                            } else if(i == Modify_Social) {
                                userBuilder.clearSocial();
                            } else if(i == Modify_Extra) {
                                userBuilder.clearExtra();
                            }
                        }
                        user = userBuilder.build();
                    }
                    resultBuilder.setUser(user);
                    resultBuilder.setCode(ProtoConstants.UserResultCode.Success);
                } else {
                    user = WFCMessage.User.newBuilder().setUid(request.getUid()).build();
                    resultBuilder.setUser(user);
                    resultBuilder.setCode(ProtoConstants.UserResultCode.NotModified);
                }
            }
            builder.addResult(resultBuilder.build());

        }

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode modifyUserInfo(String userId, WFCMessage.ModifyMyInfoRequest request) throws Exception {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);


        WFCMessage.User user = mUserMap.get(userId);
        if (user == null) {
            user = databaseStore.getPersistUser(userId);
            if (user != null) {
                mUserMap.set(userId, user);
            } else {
                return ErrorCode.ERROR_CODE_NOT_EXIST;
            }
        }

        WFCMessage.User.Builder builder = user.toBuilder();
        boolean modified = false;
        for (WFCMessage.InfoEntry entry : request.getEntryList()
            ) {
            switch (entry.getType()) {
                case Modify_DisplayName:
                    builder.setDisplayName(entry.getValue());
                    modified = true;
                    break;
                case Modify_Gender:
                    builder.setGender(Integer.parseInt(entry.getValue()));
                    modified = true;
                    break;
                case Modify_Portrait:
                    builder.setPortrait(entry.getValue());
                    modified = true;
                    break;
                //禁止客户端直接修改电话号码，只能通过admin api来修改
//                case Modify_Mobile:
//                    builder.setMobile(entry.getValue());
//                    modified = true;
//                    break;
                case Modify_Email:
                    builder.setEmail(entry.getValue());
                    modified = true;
                    break;
                case Modify_Address:
                    builder.setAddress(entry.getValue());
                    modified = true;
                    break;
                case Modify_Company:
                    builder.setCompany(entry.getValue());
                    modified = true;
                    break;
                case Modify_Social:
                    builder.setSocial(entry.getValue());
                    modified = true;
                    break;
                case Modify_Extra:
                    builder.setExtra(entry.getValue());
                    modified = true;
                    break;
                default:
                    break;
            }
        }

        if (modified) {
            builder.setUpdateDt(System.currentTimeMillis());
            user = builder.build();
            databaseStore.updateUser(user);
            mUserMap.set(userId, user);

            callbackUserInfoEvent(user);

            return ErrorCode.ERROR_CODE_SUCCESS;
        } else {
            return ErrorCode.ERROR_CODE_NOT_MODIFIED;
        }

    }

    @Override
    public void updateUserOnlineSetting(MemorySessionStore.Session session, boolean online) {
        if(!mMultiPlatformNotification) {
            return;
        }
        if (m_Server.getStore().sessionsStore().isMultiEndpointSupported()) {
            return;
        }

        if (m_Server.isShutdowning()) {
            return;
        }

        if (session.getPlatform() == Platform_LINUX || session.getPlatform() == Platform_Windows || session.getPlatform() == Platform_OSX) {
            updateUserSettings(session.username, WFCMessage.ModifyUserSettingReq.newBuilder().setScope(kUserSettingPCOnline).setKey("PC").setValue(online ? (System.currentTimeMillis()  + "|"  + session.getPlatform() + "|" + session.getClientID() + "|" + session.getPhoneName()) : "").build(), session.clientID);
        } else {
            String value = null;
            for (MemorySessionStore.Session s : m_Server.getStore().sessionsStore().sessionForUser(session.username)) {
                if (s.getDeleted() != 0 || !m_Server.getConnectionsManager().isConnected(s.getClientID())) {
                    continue;
                }

                switch (s.getPlatform()) {
                    case Platform_LINUX:
                    case Platform_Windows:
                    case Platform_OSX:
                        value = System.currentTimeMillis() + "|" + s.getPlatform() + "|" + s.getClientID() + "|" + s.getPhoneName();
                        break;
                    default:
                        break;
                }

                if (value != null) {
                    break;
                }
            }

            WFCMessage.UserSettingEntry pcentry = getUserSetting(session.getUsername(), kUserSettingPCOnline, "PC");
            if (value != null) {
                if (pcentry == null || StringUtil.isNullOrEmpty(pcentry.getValue())) {
                    updateUserSettings(session.username, WFCMessage.ModifyUserSettingReq.newBuilder().setScope(kUserSettingPCOnline).setKey("PC").setValue(value).build(), session.clientID);
                }
            } else {
                if (pcentry != null && !StringUtil.isNullOrEmpty(pcentry.getValue())) {
                    updateUserSettings(session.username, WFCMessage.ModifyUserSettingReq.newBuilder().setScope(kUserSettingPCOnline).setKey("PC").setValue("").build(), session.clientID);
                }
            }
        }
    }

    @Override
    public ErrorCode modifyUserStatus(String userId, int status) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, Integer> mUserMap = hzInstance.getMap(USER_STATUS);
        if (status == 0) {
            mUserMap.delete(userId);
        } else {
            mUserMap.put(userId, status);
        }
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public int getUserStatus(String userId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, Integer> mUserMap = hzInstance.getMap(USER_STATUS);
        Integer status = mUserMap.get(userId);

        if (status != null) {
            return status;
        }

        return 0;
    }

    @Override
    public List<InputOutputUserBlockStatus> getUserStatusList() {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, Integer> mUserMap = hzInstance.getMap(USER_STATUS);
        ArrayList<InputOutputUserBlockStatus> out = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : mUserMap.entrySet()) {
            out.add(new InputOutputUserBlockStatus(entry.getKey(), entry.getValue()));
        }
        return out;
    }

    @Override
    public ErrorCode updateUserInfo(InputOutputUserInfo userInfo, int flag) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);
        WFCMessage.User user = mUserMap.get(userInfo.getUserId());
        if(user == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }
        WFCMessage.User.Builder builder = user.toBuilder();
        if((flag & Update_User_DisplayName) > 0) {
            builder.setDisplayName(userInfo.getDisplayName());
        }
        if((flag & Update_User_Portrait) > 0) {
            builder.setPortrait(userInfo.getPortrait());
        }
        if((flag & Update_User_Gender) > 0) {
            builder.setGender(userInfo.getGender());
        }
        if((flag & Update_User_Mobile) > 0) {
            builder.setMobile(userInfo.getMobile());
        }
        if((flag & Update_User_Email) > 0) {
            builder.setEmail(userInfo.getEmail());
        }
        if((flag & Update_User_Address) > 0) {
            builder.setAddress(userInfo.getAddress());
        }
        if((flag & Update_User_Company) > 0) {
            builder.setCompany(userInfo.getCompany());
        }
        if((flag & Update_User_Social) > 0) {
            builder.setSocial(userInfo.getSocial());
        }
        if((flag & Update_User_Extra) > 0) {
            builder.setExtra(userInfo.getExtra());
        }
        if((flag & Update_User_Name) > 0) {
            if(StringUtil.isNullOrEmpty(userInfo.getName())) {
                LOG.error("Modify user info failure, name not exist!");
                return ErrorCode.INVALID_PARAMETER;
            }
            builder.setName(userInfo.getName());
        }
        builder.setUpdateDt(System.currentTimeMillis());
        user = builder.build();
        try {
            databaseStore.updateUser(user);
        } catch (Exception e) {
            e.printStackTrace();
            return ErrorCode.ERROR_CODE_SERVER_ERROR;
        }

        mUserMap.put(user.getUid(), user);
        callbackUserInfoEvent(user);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public void addUserInfo(WFCMessage.User user, String password) throws Exception {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);
        if (databaseStore.isUidAndNameConflict(user.getUid(), user.getName())) {
            throw new Exception("用户名不能重复，必须唯一！！！");
        }
        databaseStore.updateUser(user);
        mUserMap.put(user.getUid(), user);
        databaseStore.updateUserPassword(user.getUid(), password);

        callbackUserInfoEvent(user);
    }

    @Override
    public void destoryUser(String userId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);
        WFCMessage.User us = mUserMap.get(userId);
        if (us != null) {
            us = WFCMessage.User.newBuilder().setUid(userId).setUpdateDt(System.currentTimeMillis()).setDisplayName("").setName(userId).setDeleted(1).build();
            try {
                databaseStore.updateUser(us);
            } catch (Exception e) {
                e.printStackTrace();
                Utility.printExecption(LOG, e);
            }
            mUserMap.put(userId, us);
        }

    }
    @Override
    public void updateUserInfo(WFCMessage.User user) throws Exception {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);
        if(mUserMap.get(user.getUid()) != null) {
            databaseStore.updateUser(user);
            mUserMap.put(user.getUid(), user);
        }
    }

    @Override
    public WFCMessage.User getUserInfo(String userId) {
        if (StringUtil.isNullOrEmpty(userId)) {
            return null;
        }

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);
        WFCMessage.User user = mUserMap.get(userId);
        return user;
    }

    @Override
    public WFCMessage.User getUserInfoByName(String name) {
        String userId = databaseStore.getUserIdByName(name);
        return getUserInfo(userId);
    }

    @Override
    public WFCMessage.User getUserInfoByMobile(String mobile) {
        String userId = databaseStore.getUserIdByMobile(mobile);
        return getUserInfo(userId);
    }
    @Override
    public List<WFCMessage.User> searchUser(String keyword, int searchType, int page) {
        if (mDisableSearch) {
            return new ArrayList<>();
        }

        if (mDisableNicknameSearch && searchType == ProtoConstants.SearchUserType.SearchUserType_General) {
            searchType = ProtoConstants.SearchUserType.SearchUserType_Name_Mobile;
        }

        return databaseStore.searchUserFromDB(keyword, searchType, page);
    }

    @Override
    public boolean updateSystemSetting(int id, String value, String desc) {
        return databaseStore.updateSystemSetting(id, value, desc);
    }

    @Override
    public SystemSettingPojo getSystemSetting(int id) {
        return databaseStore.getSystemSetting(id);
    }

    @Override
    public void createChatroom(String chatroomId, WFCMessage.ChatroomInfo chatroomInfo) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChatroomInfo> chatroomInfoMap = hzInstance.getMap(CHATROOMS);
        WFCMessage.ChatroomInfo preInfo = chatroomInfoMap.get(chatroomId);
        if (preInfo != null) {
            WFCMessage.ChatroomInfo.Builder builder = preInfo.toBuilder();
            if (!StringUtil.isNullOrEmpty(chatroomInfo.getTitle())) {
                builder.setTitle(chatroomInfo.getTitle());
            }

            if (!StringUtil.isNullOrEmpty(chatroomInfo.getDesc())) {
                builder.setDesc(chatroomInfo.getDesc());
            }

            if (!StringUtil.isNullOrEmpty(chatroomInfo.getPortrait())) {
                builder.setPortrait(chatroomInfo.getPortrait());
            }

            if (!StringUtil.isNullOrEmpty(chatroomInfo.getExtra())) {
                builder.setExtra(chatroomInfo.getExtra());
            }

            builder.setState(chatroomInfo.getState());

            chatroomInfo = builder.build();
        }
        chatroomInfoMap.put(chatroomId, chatroomInfo);
        callbackChatroomInfoUpdateEvent(chatroomId, Chatroom_Event_Create);
    }

    @Override
    public void destoryChatroom(String chatroomId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChatroomInfo> chatroomInfoMap = hzInstance.getMap(CHATROOMS);
        WFCMessage.ChatroomInfo room = chatroomInfoMap.get(chatroomId);
        if (room != null) {
            room = room.toBuilder().setUpdateDt(System.currentTimeMillis()).setState(ProtoConstants.ChatroomState.Chatroom_State_End).build();
            chatroomInfoMap.put(chatroomId, room);
            callbackChatroomInfoUpdateEvent(chatroomId, Chatroom_Event_Destroy);
        }
    }

    @Override
    public WFCMessage.ChatroomInfo getChatroomInfo(String chatroomId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChatroomInfo> chatroomInfoMap = hzInstance.getMap(CHATROOMS);
        return chatroomInfoMap.get(chatroomId);
    }

    @Override
    public WFCMessage.ChatroomMemberInfo getChatroomMemberInfo(String chatroomId, final int maxMemberCount) {
        MultiMap<String, UserClientEntry> chatroomMembers = m_Server.getHazelcastInstance().getMultiMap(CHATROOM_MEMBER_IDS);
        Collection<UserClientEntry> members = chatroomMembers.get(chatroomId);

        if (members != null) {
            ArrayList<String> memberIds = new ArrayList<>();
            if (members.size() <= maxMemberCount) {
                    members.stream().forEach(new Consumer<UserClientEntry>() {
                        @Override
                        public void accept(UserClientEntry userClientEntry) {
                            memberIds.add(userClientEntry.userId);
                        }
                    });
            } else {
                Spliterator<UserClientEntry> p = members.spliterator();
                while (p.estimateSize() > 2*maxMemberCount) {
                    p = p.trySplit();
                }

                p.forEachRemaining(new Consumer<UserClientEntry>() {
                    public void accept(UserClientEntry s) {
                        if (memberIds.size() < maxMemberCount) {
                            memberIds.add(s.userId);
                        }
                    }
                });
            }
            return WFCMessage.ChatroomMemberInfo.newBuilder().setMemberCount(members.size()).addAllMembers(memberIds).build();
        }

        return WFCMessage.ChatroomMemberInfo.newBuilder().setMemberCount(0).build();

    }
    @Override
    public int getChatroomMemberCount(String chatroomId) {
        return m_Server.getHazelcastInstance().getMultiMap(CHATROOM_MEMBER_IDS).valueCount(chatroomId);
    }

    @Override
    public ErrorCode verifyToken(String userId, String token, List<String> serverIPs, List<Integer> ports) {
        String tokenUserId = Tokenor.getUserId(token.getBytes());

        if (tokenUserId != null) {
            if (tokenUserId.equals(userId)) {
                Member member = m_Server.getHazelcastInstance().getCluster().getLocalMember();
                String serverIp = member.getStringAttribute(HZ_Cluster_Node_External_IP);
                String longPort = member.getStringAttribute(HZ_Cluster_Node_External_Long_Port);
                String shortPort = member.getStringAttribute(HZ_Cluster_Node_External_Short_Port);
                if (!StringUtil.isNullOrEmpty(serverIp)) {
                    serverIPs.add(serverIp);
                }
                ports.add(Integer.parseInt(longPort));
                ports.add(Integer.parseInt(shortPort));
            } else {
                return ErrorCode.ERROR_CODE_TOKEN_ERROR;
            }
        } else {
            return ErrorCode.ERROR_CODE_TOKEN_ERROR;
        }
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode isAllowUserMessage(String targetUser, String fromUser) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

        Collection<FriendData> friendDatas = friendsMap.get(targetUser);

        if (friendDatas == null || friendDatas.size() == 0) {
            friendDatas = loadFriend(friendsMap, targetUser);
        }

        for (FriendData friendData : friendDatas) {
            if (friendData.getFriendUid().equals(fromUser)) {
                if (friendData.getBlacked() == 1) {
                    return ErrorCode.ERROR_CODE_IN_BLACK_LIST;
                } else {
                    if (friendData.getState() == 0) {
                        return ErrorCode.ERROR_CODE_SUCCESS;
                    } else {
                        break;
                    }
                }
            }
        }


        if (mDisableStrangerChat) {
            //在禁止私聊时，允许机器人，物联网设备及管理员进行私聊。
            IMap<String, WFCMessage.User> mUserMap = hzInstance.getMap(USERS);
            WFCMessage.User target = mUserMap.get(targetUser);
            if (target != null && target.getType() != ProtoConstants.UserType.UserType_Normal) {
                return ErrorCode.ERROR_CODE_SUCCESS;
            }
            target = mUserMap.get(fromUser);
            if (target != null && target.getType() != ProtoConstants.UserType.UserType_Normal) {
                return ErrorCode.ERROR_CODE_SUCCESS;
            }
            //admin的usertype为3，所以不需要hardcode为admin添加例外
//            if (targetUser.equals("admin") || fromUser.equals("admin")) {
//                return ErrorCode.ERROR_CODE_SUCCESS;
//            }
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    synchronized Collection<FriendData> loadFriend(MultiMap<String, FriendData> friendsMap, String userId) {
        Collection<FriendData> friends = databaseStore.getPersistFriends(userId);
        if (friends != null) {
            for (FriendData friend : friends) {
                friendsMap.put(userId, friend);
            }
        } else {
            friends = new ArrayList<>();
        }
        return friends;
    }

    synchronized Collection<WFCMessage.FriendRequest> loadFriendRequest(MultiMap<String, WFCMessage.FriendRequest> requestMap, String userId) {
        Collection<WFCMessage.FriendRequest> requests = databaseStore.getPersistFriendRequests(userId);
        if (requests != null) {
            for (WFCMessage.FriendRequest r : requests
                ) {
                requestMap.put(userId, r);
            }
        } else {
            requests = new ArrayList<>();
        }
        return requests;
    }

    @Override
    public List<FriendData> getFriendList(String userId, String clientId, long version) {
        List<FriendData> out = new ArrayList<FriendData>();

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);
        Collection<FriendData> friends = friendsMap.get(userId);
        if (friends == null || friends.size() == 0) {
            friends = loadFriend(friendsMap, userId);
        }

        boolean needFriendMigrate = false;

        if (!StringUtil.isNullOrEmpty(clientId)) {
            MemorySessionStore.Session session = m_Server.getStore().sessionsStore().getSession(clientId);
            if(session != null && session.getMqttVersion() != null && session.getMqttVersion().protocolLevel() < MqttVersion.Wildfire_2.protocolLevel()) {
                needFriendMigrate = true;
            }
        }

        for (FriendData friend : friends) {
            if (friend.getTimestamp() > version) {
                if (needFriendMigrate) {
                    if (friend.getBlacked() > 0) {
                        friend.setState(2);
                    }
                    out.add(friend);
                } else {
                    out.add(friend);
                }

            }
        }

        if(mSyncDataPartSize > 0 && out.size() > mSyncDataPartSize) {
            out.sort(Comparator.comparingLong(FriendData::getTimestamp));
            out = out.subList(0, mSyncDataPartSize);
        }

        return out;
    }

    @Override
    public void clearUserFriend(String userId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

        Collection<FriendData> friendDatas = friendsMap.get(userId);
        if (friendDatas == null || friendDatas.size() == 0) {
            friendDatas = loadFriend(friendsMap, userId);
        }

        databaseStore.removeUserFriend(userId);
        databaseStore.removeUserFriendRequest(userId);

        MultiMap<String, WFCMessage.FriendRequest> requestMap = hzInstance.getMultiMap(USER_FRIENDS_REQUEST);
        requestMap.remove(userId);


        if (friendDatas != null) {
            for (FriendData fd : friendDatas) {
                friendsMap.remove(fd.getFriendUid());
            }
            friendsMap.remove(userId);
        }
    }

    @Override
    public List<WFCMessage.FriendRequest> getFriendRequestList(String userId, long version) {
        List<WFCMessage.FriendRequest> out = new ArrayList<>();

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.FriendRequest> requestMap = hzInstance.getMultiMap(USER_FRIENDS_REQUEST);
        Collection<WFCMessage.FriendRequest> requests = requestMap.get(userId);
        if (requests == null || requests.size() == 0) {
            requests = loadFriendRequest(requestMap, userId);
        }

        for (WFCMessage.FriendRequest request : requests) {
            if (request.getUpdateDt() > version) {
                out.add(request);
            }
        }

        if(mSyncDataPartSize > 0 && out.size() > mSyncDataPartSize) {
            out.sort(Comparator.comparingLong(WFCMessage.FriendRequest::getUpdateDt));
            out = out.subList(0, mSyncDataPartSize);
        }
        return out;
    }

    @Override
    public ErrorCode saveAddFriendRequest(String userId, WFCMessage.AddFriendRequest request, long[] head, boolean isAdmin) {
        if (mDisableFriendRequest) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.FriendRequest> requestMap = hzInstance.getMultiMap(USER_FRIENDS_REQUEST);
        Collection<WFCMessage.FriendRequest> requests = requestMap.get(userId);
        if (requests == null || requests.size() == 0) {
            requests = loadFriendRequest(requestMap, userId);
        }

        WFCMessage.FriendRequest existRequest = null;

        for (WFCMessage.FriendRequest tmpRequest : requests) {
            if (tmpRequest.getToUid().equals(request.getTargetUid())) {
                existRequest = tmpRequest;
                break;
            }
        }

        if (existRequest != null && existRequest.getStatus() != ProtoConstants.FriendRequestStatus.RequestStatus_Accepted && !isAdmin) {
            if (mFriendRequestDuration > 0 && System.currentTimeMillis() - existRequest.getUpdateDt() > mFriendRequestDuration) {
                if (existRequest.getStatus() == ProtoConstants.FriendRequestStatus.RequestStatus_Rejected
                    && System.currentTimeMillis() - existRequest.getUpdateDt() < mFriendRejectDuration) {
                    return ErrorCode.ERROR_CODE_FRIEND_REQUEST_BLOCKED;
                }
            } else {
                return ErrorCode.ERROR_CODE_FRIEND_ALREADY_REQUEST;
            }
        }

        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

        FriendData friendData1 = null;
        Collection<FriendData> friendDatas = friendsMap.get(userId);
        if (friendDatas == null || friendDatas.size() == 0) {
            friendDatas = loadFriend(friendsMap, userId);
        }
        for (FriendData fd : friendDatas) {
            if (fd.getFriendUid().equals(request.getTargetUid())) {
                friendData1 = fd;
                break;
            }
        }
        if (friendData1 != null && friendData1.getBlacked() > 0 && !isAdmin) {
            return ErrorCode.ERROR_CODE_IN_BLACK_LIST;
        }

        WFCMessage.FriendRequest newRequest = WFCMessage.FriendRequest
            .newBuilder()
            .setFromUid(userId)
            .setToUid(request.getTargetUid())
            .setReason(request.getReason())
            .setExtra(request.getExtra())
            .setStatus(ProtoConstants.FriendRequestStatus.RequestStatus_Sent)
            .setToReadStatus(false)
            .setUpdateDt(System.currentTimeMillis())
            .build();

        databaseStore.persistOrUpdateFriendRequest(newRequest);
        requestMap.remove(userId);
        requestMap.remove(request.getTargetUid());

        head[0] = newRequest.getUpdateDt();
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode SyncFriendRequestUnread(String userId, long unreadDt, long[] head) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.FriendRequest> requestMap = hzInstance.getMultiMap(USER_FRIENDS_REQUEST);

        long curTS = System.currentTimeMillis();
        head[0] = curTS;

        databaseStore.persistFriendRequestUnreadStatus(userId, unreadDt, curTS);
        requestMap.remove(userId);

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode handleFriendRequest(String userId, WFCMessage.HandleFriendRequest request, WFCMessage.Message.Builder msgBuilder, long[] heads, boolean isAdmin) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();


        if(!isAdmin && request.getStatus() == ProtoConstants.FriendRequestStatus.RequestStatus_Accepted) {
            MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

            Collection<FriendData> friends = friendsMap.get(userId);
            if (friends == null || friends.size() == 0) {
                friends = loadFriend(friendsMap, userId);
            }

            for (FriendData fd : friends) {
                if (fd.getFriendUid().equals(request.getTargetUid())) {
                    if (fd.getState() == 0) {
                        return ErrorCode.ERROR_CODE_ALREADY_FRIENDS;
                    } else {
                        break;
                    }
                }
            }
        }

        if (isAdmin) {
            MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

            FriendData friendData1 = null;
            Collection<FriendData> friendDatas = friendsMap.get(userId);
            if (friendDatas == null || friendDatas.size() == 0) {
                friendDatas = loadFriend(friendsMap, userId);
            }
            for (FriendData fd : friendDatas) {
                if (fd.getFriendUid().equals(request.getTargetUid())) {
                    friendData1 = fd;
                    break;
                }
            }
            if (friendData1 == null) {
                friendData1 = new FriendData(userId, request.getTargetUid(), "", request.getExtra(), request.getStatus(), 0, System.currentTimeMillis());
            } else {
                friendData1.setState(request.getStatus());
                friendData1.setExtra(request.getExtra());
                friendData1.setTimestamp(System.currentTimeMillis());
            }

            databaseStore.persistOrUpdateFriendData(friendData1);

            if (request.getStatus() != 2) {
                FriendData friendData2 = null;

                friendDatas = friendsMap.get(request.getTargetUid());
                if (friendDatas == null || friendDatas.size() == 0) {
                    friendDatas = loadFriend(friendsMap, request.getTargetUid());
                }
                for (FriendData fd : friendDatas) {
                    if (fd.getFriendUid().equals(userId)) {
                        friendData2 = fd;
                        break;
                    }
                }
                if (friendData2 == null) {
                    friendData2 = new FriendData(request.getTargetUid(), userId, "", request.getExtra(), request.getStatus(), 0, friendData1.getTimestamp());
                } else {
                    friendsMap.remove(request.getTargetUid(), friendData2);
                    friendData2.setState(request.getStatus());
                    friendData2.setTimestamp(System.currentTimeMillis());
                }

                databaseStore.persistOrUpdateFriendData(friendData2);

                heads[0] = friendData2.getTimestamp();
            } else {
                heads[0] = 0;
            }
            heads[1] = friendData1.getTimestamp();
            friendsMap.remove(userId);
            friendsMap.remove(request.getTargetUid());

            callbackRelationEvent(userId, request.getTargetUid(), 0, "" + request.getStatus());
            return ErrorCode.ERROR_CODE_SUCCESS;
        }

        MultiMap<String, WFCMessage.FriendRequest> requestMap = hzInstance.getMultiMap(USER_FRIENDS_REQUEST);
        Collection<WFCMessage.FriendRequest> requests = requestMap.get(userId);
        if (requests == null || requests.size() == 0) {
            requests = loadFriendRequest(requestMap, userId);
        }

        WFCMessage.FriendRequest existRequest = null;
        for (WFCMessage.FriendRequest tmpRequest : requests) {
            if (tmpRequest.getFromUid().equals(request.getTargetUid())) {
                existRequest = tmpRequest;
                break;
            }
        }

        if (existRequest != null) {
            if (mFriendRequestExpiration > 0 && System.currentTimeMillis() - existRequest.getUpdateDt() > mFriendRequestExpiration) {
                return ErrorCode.ERROR_CODE_FRIEND_REQUEST_EXPIRED;
            } else {
                existRequest = existRequest.toBuilder().setStatus(request.getStatus()).setUpdateDt(System.currentTimeMillis()).build();
                databaseStore.persistOrUpdateFriendRequest(existRequest);

                if (request.getStatus() == ProtoConstants.FriendRequestStatus.RequestStatus_Accepted) {
                    Collection<WFCMessage.FriendRequest> targetRequests = requestMap.get(request.getTargetUid());
                    if (targetRequests == null || targetRequests.size() == 0) {
                        targetRequests = loadFriendRequest(requestMap, request.getTargetUid());
                    }

                    WFCMessage.FriendRequest existTargetRequest = null;
                    for (WFCMessage.FriendRequest tmpRequest : targetRequests) {
                        if (tmpRequest.getFromUid().equals(userId)) {
                            existTargetRequest = tmpRequest;
                            break;
                        }
                    }

                    if (existTargetRequest != null && existTargetRequest.getStatus() == ProtoConstants.FriendRequestStatus.RequestStatus_Sent) {
                        existTargetRequest = existTargetRequest.toBuilder().setStatus(request.getStatus()).setUpdateDt(existRequest.getUpdateDt()).build();
                        databaseStore.persistOrUpdateFriendRequest(existTargetRequest);
                    }
                }

                if(request.getStatus() == ProtoConstants.FriendRequestStatus.RequestStatus_Accepted){
                    MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);
                    FriendData friendData1 = new FriendData(userId, request.getTargetUid(), "", request.getExtra(), 0, 0, System.currentTimeMillis());
                    databaseStore.persistOrUpdateFriendData(friendData1);

                    FriendData friendData2 = new FriendData(request.getTargetUid(), userId, "", request.getExtra(), 0, 0, friendData1.getTimestamp());
                    databaseStore.persistOrUpdateFriendData(friendData2);

                    requestMap.remove(userId);
                    requestMap.remove(request.getTargetUid());
                    friendsMap.remove(userId);
                    friendsMap.remove(request.getTargetUid());

                    heads[0] = friendData2.getTimestamp();
                    heads[1] = friendData1.getTimestamp();

                    msgBuilder.setConversation(WFCMessage.Conversation.newBuilder().setTarget(userId).setLine(0).setType(ProtoConstants.ConversationType.ConversationType_Private).build());
                    msgBuilder.setContent(WFCMessage.MessageContent.newBuilder().setType(1).setSearchableContent(existRequest.getReason()).build());

                    callbackRelationEvent(userId, request.getTargetUid(), 0, "1");
                }
                return ErrorCode.ERROR_CODE_SUCCESS;
            }
        } else {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }
    }

    @Override
    public ErrorCode blackUserRequest(String fromUser, String targetUserId, int state, long[] heads) {
        if(state != 0 && state != 1 && state != 2){
            return ErrorCode.INVALID_PARAMETER;
        }

        if (state == 2) {
            state = 1;
        } else {
            state = 0;
        }

        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

        FriendData friendData = null;
        Collection<FriendData> friends = friendsMap.get(fromUser);
        if (friends == null || friends.size() == 0) {
            friends = loadFriend(friendsMap, fromUser);
        }

        for (FriendData fd:friends) {
            if (fd.getFriendUid().equals(targetUserId)) {
                friendData = fd;
                break;
            }
        }

        if (friendData == null) {
            friendData = new FriendData(fromUser, targetUserId, "", "", 1, state, System.currentTimeMillis());
        }
        friendData.setBlacked(state);
        friendData.setTimestamp(System.currentTimeMillis());


        databaseStore.persistOrUpdateFriendData(friendData);
        friendsMap.remove(fromUser);

        heads[0] = friendData.getTimestamp();

        callbackRelationEvent(fromUser, targetUserId, 2, state+"");
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public FriendData getFriendData(String fromUser, String targetUserId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

        Collection<FriendData> friends = friendsMap.get(fromUser);
        if (friends == null || friends.size() == 0) {
            friends = loadFriend(friendsMap, fromUser);
        }

        for (FriendData fd:friends) {
            if (fd.getFriendUid().equals(targetUserId)) {
                return fd;
            }
        }

        return null;
    }

    @Override
    public ErrorCode setFriendAliasRequest(String fromUser, String targetUserId, String alias, long[] heads){
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);

        FriendData friendData = null;
        Collection<FriendData> friends = friendsMap.get(fromUser);
        if (friends == null || friends.size() == 0) {
            friends = loadFriend(friendsMap, fromUser);
        }

        for (FriendData fd:friends) {
            if (fd.getFriendUid().equals(targetUserId)) {
                friendData = fd;
                break;
            }
        }

        if (friendData == null) {
            friendData = new FriendData();
            friendData.setUserId(fromUser);
            friendData.setFriendUid(targetUserId);
        }

        friendData.setAlias(alias);
        friendData.setTimestamp(System.currentTimeMillis());

        databaseStore.persistOrUpdateFriendData(friendData);

        heads[0] = friendData.getTimestamp();

        friendsMap.remove(fromUser);

        callbackRelationEvent(fromUser, targetUserId, 1, alias);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode handleJoinChatroom(String userId, String clientId, String chatroomId) {
        IMap<String, WFCMessage.ChatroomInfo> chatroomInfoMap = m_Server.getHazelcastInstance().getMap(CHATROOMS);
        if (chatroomInfoMap == null || chatroomInfoMap.get(chatroomId) == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        m_Server.getStore().sessionsStore().getSession(clientId).refreshLastChatroomActiveTime();
        m_Server.getHazelcastInstance().getMap(USER_CHATROOM).put(userId, chatroomId);
        m_Server.getHazelcastInstance().getMultiMap(CHATROOM_MEMBER_IDS).put(chatroomId, new UserClientEntry(userId, clientId));

        mWriteLock.lock();
        chatroomMessages.put(userId, new TreeMap<>());
        mWriteLock.unlock();

        callbackChatroomMemberEvent(userId, chatroomId, Arrays.asList(userId), Chatroom_Member_Event_Join);

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode handleQuitChatroom(String userId, String clientId, String chatroomId) {
        m_Server.getHazelcastInstance().getMap(USER_CHATROOM).remove(userId);
        m_Server.getHazelcastInstance().getMultiMap(CHATROOM_MEMBER_IDS).remove(chatroomId, new UserClientEntry(userId, clientId));

        mWriteLock.lock();
        chatroomMessages.remove(userId);
        mWriteLock.unlock();

        callbackChatroomMemberEvent(userId, chatroomId, Arrays.asList(userId), Chatroom_Member_Event_Leave);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public boolean checkChatroomParticipantIdelTime(MemorySessionStore.Session session) {
        if (session == null) {
            return false;
        }

        if(mChatroomParticipantIdleTime > 0 && System.currentTimeMillis() - session.getLastChatroomActiveTime() > mChatroomParticipantIdleTime) {
            return false;
        }
        return true;
    }

    @Override
    public boolean isNewFriendWelcomeMessage() {
        return mFriendNewWelcomeMessage;
    }

    @Override
    public ErrorCode deleteFriend(String userId, String friendUid, long[] head) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);
        Collection<FriendData> user1Friends = friendsMap.get(userId);
        if (user1Friends == null || user1Friends.size() == 0) {
            user1Friends = loadFriend(friendsMap, userId);
        }
        for (FriendData data :
            user1Friends) {
            if (data.getFriendUid().equals(friendUid)) {
                long ts = System.currentTimeMillis();
                head[0] = ts;
                data.setState(1);
                data.setTimestamp(ts);
                databaseStore.persistOrUpdateFriendData(data);
                break;
            }
        }

        Collection<FriendData> user2Friends = friendsMap.get(friendUid);
        if (user2Friends == null || user2Friends.size() == 0) {
            user2Friends = loadFriend(friendsMap, friendUid);
        }
        for (FriendData data :
            user2Friends) {
            if (data.getFriendUid().equals(userId)) {
                data.setState(1);
                long ts = System.currentTimeMillis();
                head[1] = ts;
                data.setTimestamp(ts);
                databaseStore.persistOrUpdateFriendData(data);
                break;
            }
        }

        friendsMap.remove(userId);
        friendsMap.remove(friendUid);
        callbackRelationEvent(userId, friendUid, 0, "0");
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode getUserSettings(String userId, long version, WFCMessage.GetUserSettingResult.Builder builder) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);
        IMap<String, WFCMessage.GroupInfo> mGroups = hzInstance.getMap(GROUPS_MAP);
        IMap<String, WFCMessage.ChannelInfo> mChannels = hzInstance.getMap(CHANNELS);

        Collection<WFCMessage.UserSettingEntry> entries = userSettingMap.get(userId);
        if (entries == null || entries.size() == 0) {
            entries = loadPersistedUserSettings(userId, userSettingMap);
        }

        ErrorCode ec = ErrorCode.ERROR_CODE_NOT_MODIFIED;
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.MONTH, -1);
        long monthAgo = c.getTimeInMillis();

        if (entries != null) {
            for (WFCMessage.UserSettingEntry entry : entries
            ) {
                if (entry.getUpdateDt() > version) {
                    ec = ErrorCode.ERROR_CODE_SUCCESS;
                    boolean skip = false;
                    if (version == 0 && entry.getUpdateDt() > monthAgo) {
                        if (entry.getScope() == UserSettingScope.kUserSettingConversationSync) {
                            try {
                                String key = entry.getKey();
                                int pos1 = key.indexOf('-');
                                String typeStr = key.substring(0, pos1);
                                int type = Integer.parseInt(typeStr);
                                if (type == ProtoConstants.ConversationType.ConversationType_Group
                                    || type == ProtoConstants.ConversationType.ConversationType_Channel) {
                                    int pos2 = key.indexOf('-', pos1 + 1);
                                    String target = key.substring(pos2 + 1);

                                    if (type == ProtoConstants.ConversationType.ConversationType_Group) {
                                        WFCMessage.GroupInfo groupInfo = mGroups.get(target);
                                        if (groupInfo == null) {
                                            skip = true;
                                        }
                                    } else {
                                        WFCMessage.ChannelInfo channelInfo = mChannels.get(target);
                                        if (channelInfo == null) {
                                            skip = true;
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                Utility.printExecption(LOG, e);
                            }

                        }
                    }

                    if (!skip) {
                        builder.addEntry(entry);
                    }
                }
            }
        }

        if(mSyncDataPartSize > 0 && ec == ErrorCode.ERROR_CODE_SUCCESS && builder.getEntryCount() > mSyncDataPartSize) {
            List<WFCMessage.UserSettingEntry> list = new ArrayList<>(builder.getEntryList());
            list.sort(Comparator.comparingLong(WFCMessage.UserSettingEntry::getUpdateDt));
            list = list.subList(0, mSyncDataPartSize);
            builder.clearEntry().addAllEntry(list);
        }
        return ec;
    }


    private List<WFCMessage.UserSettingEntry> loadPersistedUserSettings(String userId, MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap) {
        List<WFCMessage.UserSettingEntry> datas = databaseStore.getPersistUserSetting(userId);
        for (WFCMessage.UserSettingEntry data : datas
             ) {
            userSettingMap.put(userId, data);
        }
        return datas;
    }


    @Override
    public WFCMessage.UserSettingEntry getUserSetting(String userId, int scope, String key) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);

        Collection<WFCMessage.UserSettingEntry> entries = userSettingMap.get(userId);
        if (entries == null || entries.size() == 0) {
            entries = loadPersistedUserSettings(userId, userSettingMap);
        }

        if (entries != null) {
            for (WFCMessage.UserSettingEntry entry : entries) {
                if (entry.getScope() == scope && (key== null || key.equals(entry.getKey()))) {
                    return entry;
                }
            }
        }

        return null;
    }

    @Override
    public List<WFCMessage.UserSettingEntry> getUserSetting(String userId, int scope) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);

        Collection<WFCMessage.UserSettingEntry> entries = userSettingMap.get(userId);
        if (entries == null || entries.size() == 0) {
            entries = loadPersistedUserSettings(userId, userSettingMap);
        }

        List<WFCMessage.UserSettingEntry> result = new ArrayList<>();
        if (entries != null) {
            for (WFCMessage.UserSettingEntry entry : entries
            ) {
                if (entry.getScope() == scope) {
                    result.add(entry);
                }
            }
        }

        return result;
    }

    @Override
    public long updateUserSettings(String userId, WFCMessage.ModifyUserSettingReq request, String clientId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);

        Collection<WFCMessage.UserSettingEntry> entries = userSettingMap.get(userId);
        if (entries == null || entries.size() == 0) {
            entries = loadPersistedUserSettings(userId, userSettingMap);
            if (entries == null) {
                entries = new ArrayList<>();
            }
        }

        long updateDt = System.currentTimeMillis();
        WFCMessage.UserSettingEntry settingEntry = WFCMessage.UserSettingEntry.newBuilder().setScope(request.getScope()).setKey(request.getKey()).setValue(request.getValue()).setUpdateDt(updateDt).build();
        databaseStore.persistUserSetting(userId, settingEntry);

        for (WFCMessage.UserSettingEntry entry : entries
            ) {
            if (entry.getScope() == request.getScope() && entry.getKey().equals(request.getKey())) {
                userSettingMap.remove(userId, entry);
                userSettingMap.put(userId, settingEntry);
                break;
            }
        }

        userSettingMap.put(userId, settingEntry);

        if(request.getScope() == UserSettingScope.kUserSettingConversationSilent) {
            int firstSplit = request.getKey().indexOf("-");
            int type = Integer.parseInt(request.getKey().substring(0, firstSplit));
            int secondSplit = request.getKey().indexOf("-", firstSplit+1);
            int line = Integer.parseInt(request.getKey().substring(firstSplit+1, secondSplit));
            String target = request.getKey().substring(secondSplit+1);
            String key = userId + "|" + type + "|" + target + "|" + line;
            userConvSlientMap.remove(key);
        } else if(request.getScope() == UserSettingScope.kUserSettingGlobalSilent) {
            userGlobalSlientMap.remove(userId);
        } else if(request.getScope() == UserSettingScope.kUserSettingHiddenNotificationDetail) {
            userPushHiddenDetail.remove(userId);
        }
        IMHandler.getPublisher().publishNotification(IMTopic.NotifyUserSettingTopic, userId, updateDt, clientId);
        return updateDt;
    }

    @Override
    public void clearUserSettings(String userId) {
        databaseStore.clearUserSetting(userId);
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);
        userSettingMap.remove(userId);
    }

    @Override
    public boolean getUserGlobalSilent(String userId) {
        Boolean slient = userGlobalSlientMap.get(userId);
        if (slient == null) {
            WFCMessage.UserSettingEntry entry = getUserSetting(userId, UserSettingScope.kUserSettingGlobalSilent, null);
            if (entry == null || !entry.getValue().equals("1")) {
                slient = false;
            } else {
                slient = true;
            }
            userGlobalSlientMap.put(userId, slient);
        }
        return slient;
    }

    @Override
    public boolean getUserPushHiddenDetail(String userId) {
        Boolean hidden = userPushHiddenDetail.get(userId);
        if (hidden == null) {
            WFCMessage.UserSettingEntry entry = getUserSetting(userId, UserSettingScope.kUserSettingHiddenNotificationDetail, null);
            if (entry == null || !entry.getValue().equals("1")) {
                hidden = false;
            } else {
                hidden = true;
            }
            userPushHiddenDetail.put(userId, hidden);
        }
        return hidden;
    }

    @Override
    public boolean getUserConversationSilent(String userId, WFCMessage.Conversation conversation) {
        String key = userId + "|" + conversation.getType() + "|" + conversation.getTarget() + "|" + conversation.getLine();
        Boolean slient = userConvSlientMap.get(key);
        if (slient == null) {
            String convSlientKey = conversation.getType() + "-" + conversation.getLine() + "-" + conversation.getTarget();
            WFCMessage.UserSettingEntry entry = getUserSetting(userId, UserSettingScope.kUserSettingConversationSilent, convSlientKey);
            if (entry == null || !entry.getValue().equals("1")) {
                slient = false;
            } else {
                slient = true;
            }
            userConvSlientMap.put(key, slient);
        }
        return slient;
    }

    @Override
    public boolean getSilentWhenPcOnline(String userId) {
        WFCMessage.UserSettingEntry entry = getUserSetting(userId, UserSettingScope.kUserSettingMuteWhenPCOnline, null);
        if (entry != null && "1".equals(entry.getValue())) {
            return true;
        } else {
            return false;
        }
    }
    private Date getTodayDate() {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);
        calendar.set(year, month, day, 0, 0, 0);
        return calendar.getTime();
    }

    @Override
    public boolean isUserNoDisturbing(String userId) {
        WFCMessage.UserSettingEntry entry = getUserSetting(userId, UserSettingScope.kUserSettingNoDisturbing, "");
        if (entry != null && !StringUtil.isNullOrEmpty(entry.getValue())) {
            String[] arr = entry.getValue().split("\\|");

            if (arr.length == 2) {
                int nowMins = (int)((System.currentTimeMillis() - getTodayDate().getTime())/1000/60);
                try {
                    int startMins = Integer.parseInt(arr[0]);
                    int endMins = Integer.parseInt(arr[1]);
                    if (endMins > startMins) {
                        if (endMins > nowMins && nowMins > startMins) {
                            return true;
                        }
                    } else {
                        if (endMins > nowMins || nowMins > startMins) {
                            return true;
                        }
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    @Override
    public ErrorCode createChannel(String operator, WFCMessage.ChannelInfo channelInfo) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChannelInfo> mIMap = hzInstance.getMap(CHANNELS);

        mIMap.put(channelInfo.getTargetId(), channelInfo);

        callbackChannelInfoUpdateEvent(operator, channelInfo.getTargetId(), Channel_Event_Create);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public void clearUserChannels(String userId) {

    }


    @Override
    public ErrorCode modifyChannelInfo(String operator, String channelId, int modifyType, String value) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChannelInfo> mIMap = hzInstance.getMap(CHANNELS);

        WFCMessage.ChannelInfo oldInfo = mIMap.get(channelId);

        if (oldInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (oldInfo.getOwner() == null || !oldInfo.getOwner().equals(operator)) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }



        WFCMessage.ChannelInfo.Builder newInfoBuilder = oldInfo.toBuilder();

        if (modifyType == Modify_Channel_Name)
            newInfoBuilder.setName(value);
        else if(modifyType == Modify_Channel_Portrait)
            newInfoBuilder.setPortrait(value);
        else if(modifyType == Modify_Channel_Desc)
            newInfoBuilder.setDesc(value);
        else if(modifyType == Modify_Channel_Extra)
            newInfoBuilder.setExtra(value);
        else if(modifyType == Modify_Channel_Secret)
            newInfoBuilder.setSecret(value);
        else if(modifyType == Modify_Channel_Callback)
            newInfoBuilder.setCallback(value);
        else if(modifyType == Modify_Channel_OnlyCallback) {
            try {
                newInfoBuilder.setAutomatic(Integer.parseInt(value));
            } catch (Exception e) {
                e.printStackTrace();
                Utility.printExecption(LOG, e);
            }
        }


        newInfoBuilder.setUpdateDt(System.currentTimeMillis());
        mIMap.put(channelId, newInfoBuilder.build());
        callbackChannelInfoUpdateEvent(operator, channelId, Channel_Event_Update);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }


    @Override
    public ErrorCode transferChannel(String operator, String channelId, String newOwner) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChannelInfo> mIMap = hzInstance.getMap(CHANNELS);

        WFCMessage.ChannelInfo oldInfo = mIMap.get(channelId);

        if (oldInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (oldInfo.getOwner() == null || !oldInfo.getOwner().equals(operator)) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }



        WFCMessage.ChannelInfo.Builder newInfoBuilder = oldInfo.toBuilder();

        newInfoBuilder.setOwner(newOwner);
        newInfoBuilder.setUpdateDt(System.currentTimeMillis());
        mIMap.put(channelId, newInfoBuilder.build());

        callbackChannelInfoUpdateEvent(operator, channelId, Channel_Event_Transfer);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public ErrorCode distoryChannel(String operator, String channelId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChannelInfo> mIMap = hzInstance.getMap(CHANNELS);

        WFCMessage.ChannelInfo oldInfo = mIMap.get(channelId);

        if (oldInfo == null) {
            return ErrorCode.ERROR_CODE_NOT_EXIST;
        }

        if (oldInfo.getOwner() == null || !oldInfo.getOwner().equals(operator)) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }


        WFCMessage.ChannelInfo.Builder newInfoBuilder = oldInfo.toBuilder();

        newInfoBuilder.setStatus(oldInfo.getStatus() | Channel_State_Mask_Deleted);
        newInfoBuilder.setUpdateDt(System.currentTimeMillis());
        mIMap.put(channelId, newInfoBuilder.build());

        callbackChannelInfoUpdateEvent(operator, channelId, Channel_Event_Destroy);
        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    @Override
    public List<WFCMessage.ChannelInfo> searchChannel(String keyword, boolean buzzy, int page) {
        return databaseStore.searchChannelFromDB(keyword, buzzy, page);
    }

    @Override
    public ErrorCode listenChannel(String operator, String channelId, boolean listen) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, String> listeners = hzInstance.getMultiMap(CHANNEL_LISTENERS);
        IMap<String, WFCMessage.ChannelInfo> mIMap = hzInstance.getMap(CHANNELS);
        WFCMessage.ChannelInfo channelInfo = mIMap.get(channelId);
        if(channelInfo != null && (channelInfo.getStatus() & ProtoConstants.ChannelState.Channel_State_Mask_Private) > 0) {
            return ErrorCode.ERROR_CODE_NOT_RIGHT;
        }

        getChannelListener(channelId);

        if (listen) {
            listeners.put(channelId, operator);
        } else {
            listeners.remove(channelId, operator);
        }
        databaseStore.updateChannelListener(channelId, operator, listen);

        return ErrorCode.ERROR_CODE_SUCCESS;
    }

    private Collection<String> getChannelListener(String channelId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, String> listeners = hzInstance.getMultiMap(CHANNEL_LISTENERS);
        Collection<String> result = listeners.get(channelId);
        if (result.isEmpty()) {
            result = databaseStore.getChannelListener(channelId);
            for (String userId : result) {
                listeners.put(channelId, userId);
            }
        }
        return result;
    }

    @Override
    public WFCMessage.ChannelInfo getChannelInfo(String channelId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<String, WFCMessage.ChannelInfo> mIMap = hzInstance.getMap(CHANNELS);
        return mIMap.get(channelId);
    }

    @Override
    public boolean checkUserInChannel(String user, String channelId) {
        Collection<String> channelMembers = getChannelListener(channelId);
        if (channelMembers == null) {
            return false;
        }

        if(!channelMembers.contains(user)) {
            IMap<String, WFCMessage.ChannelInfo> mIMap = m_Server.getHazelcastInstance().getMap(CHANNELS);
            WFCMessage.ChannelInfo info = mIMap.get(channelId);
            if (info == null || !info.getOwner().equals(user)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Collection<String> getChannelSubscriber(String channelId) {
        MultiMap<String, String> channelMembers = m_Server.getHazelcastInstance().getMultiMap(CHANNEL_LISTENERS);
        if (channelMembers == null) {
            return new ArrayList<>();
        }

        return channelMembers.get(channelId);
    }

    @Override
    public Set<String> handleSensitiveWord(String message) {
        updateSensitiveWord();
        return mSensitiveFilter.getSensitiveWords(message, SensitiveFilter.MatchType.MAX_MATCH);
    }

    @Override
    public boolean addSensitiveWords(List<String> words) {
        for (String word :
            words) {
            databaseStore.persistSensitiveWord(word.toLowerCase());
        }
        lastUpdateSensitiveTime = 0;
        return true;
    }

    @Override
    public boolean removeSensitiveWords(List<String> words) {
        for (String word :
            words) {
            databaseStore.deleteSensitiveWord(word);
        }
        lastUpdateSensitiveTime = 0;
        return true;
    }

    @Override
    public List<String> getAllSensitiveWords() {
        return new ArrayList<>(databaseStore.getSensitiveWord());
    }

    @Override
    public WFCMessage.Message getMessage(long messageId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        IMap<Long, MessageBundle> mIMap = hzInstance.getMap(MESSAGES_MAP);
        MessageBundle bundle = mIMap.get(messageId);
        if (bundle != null) {
            return bundle.getMessage();
        }
        return null;
    }

    @Override
    public boolean isAllowClientCustomGroupNotification() {
        return mGroupAllowClientCustomOperationNotification;
    }

    @Override
    public int getVisibleQuitKickoffNotification() {
        return mGroupVisibleQuitKickoffNotification;
    }


    @Override
    public List<Integer> getClientForbiddenSendTypes() {
        return mForbiddenClientSendTypes;
    }

    @Override
    public List<Integer> getBlackListExceptionTypes() {
        return mBlackListExceptionTypes;
    }

    @Override
    public List<Integer> getGroupMuteExceptionTypes() {
        return mGroupMuteExceptionTypes;
    }

    @Override
    public List<Integer> getGlobalMuteExceptionTypes() {
        return mGlobalMuteExceptionTypes;
    }

    @Override
    public long getMessageHead(String user) {
        TreeMap<Long, Long> maps = userMessages.get(user);
        if (maps == null) {
            loadUserMessages(user);
        }

        mReadLock.lock();
        try {
            maps = userMessages.get(user);
            Map.Entry<Long, Long> lastEntry = maps.lastEntry();
            if (lastEntry != null) {
                return lastEntry.getKey();
            }
        } finally {
            mReadLock.unlock();
        }
        return 0;
    }

    private void loadUserMessages(String user) {
        mWriteLock.lock();
        try {
            TreeMap<Long, Long> maps = userMessages.get(user);
            if (maps == null) {
                maps = databaseStore.reloadUserMessageMaps(user);
                userMessages.put(user, maps);
            }
        } finally {
            mWriteLock.unlock();
        }
    }

    @Override
    public long getFriendHead(String userId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, FriendData> friendsMap = hzInstance.getMultiMap(USER_FRIENDS);
        Collection<FriendData> friends = friendsMap.get(userId);
        if (friends == null || friends.size() == 0) {
            friends = loadFriend(friendsMap, userId);
        }
        long max = 0;
        if (friends != null && friends.size() > 0) {
            for (FriendData friend :
                friends) {
                if (friend.getTimestamp() > max)
                    max = friend.getTimestamp();
            }
        }

        return max;
    }

    @Override
    public long getFriendRqHead(String userId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.FriendRequest> friendsReqMap = hzInstance.getMultiMap(USER_FRIENDS_REQUEST);
        Collection<WFCMessage.FriendRequest> friendsReq = friendsReqMap.get(userId);
        long max = 0;
        if (friendsReq == null || friendsReq.size() == 0) {
            friendsReq = loadFriendRequest(friendsReqMap, userId);
        }

        if (friendsReq != null && friendsReq.size() > 0) {
            for (WFCMessage.FriendRequest request :
                friendsReq) {
                if (request.getUpdateDt() > max)
                    max = request.getUpdateDt();
            }
        }

        return max;
    }

    @Override
    public long getSettingHead(String userId) {
        HazelcastInstance hzInstance = m_Server.getHazelcastInstance();
        MultiMap<String, WFCMessage.UserSettingEntry> userSettingMap = hzInstance.getMultiMap(USER_SETTING);

        Collection<WFCMessage.UserSettingEntry> entries = userSettingMap.get(userId);
        if (entries == null || entries.size() == 0) {
            return 0L;
        }

        long max = 0;
        for (WFCMessage.UserSettingEntry entry : entries
            ) {
            if (entry.getUpdateDt() > max) {
                max = entry.getUpdateDt();
            }
        }

        return max;
    }


    @Override
    public String getShortUUID(){
        int id = databaseStore.getGeneratedId();
        if (id > 0) {
            return IDUtils.toUid(id);
        }
        return UUIDGenerator.getUUID();
    }

    @Override
    public void storeRetained(Topic topic, StoredMessage storedMessage) {
        LOG.debug("Store retained message for topic={}, CId={}", topic, storedMessage.getClientID());
        if (storedMessage.getClientID() == null) {
            throw new IllegalArgumentException("Message to be persisted must have a not null client ID");
        }
        m_retainedStore.put(topic, storedMessage);
    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        LOG.debug("searchMatching scanning all retained messages, presents are {}", m_retainedStore.size());

        List<StoredMessage> results = new ArrayList<>();

        for (Map.Entry<Topic, StoredMessage> entry : m_retainedStore.entrySet()) {
            StoredMessage storedMsg = entry.getValue();
            if (condition.match(entry.getKey())) {
                results.add(storedMsg);
            }
        }

        return results;
    }

    @Override
    public void cleanRetained(Topic topic) {
        m_retainedStore.remove(topic);
    }


    public static String getName(int[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            char ch = (char)(bytes[i]+17);
            sb.append(ch);
        }
        return sb.toString();
    }
}
